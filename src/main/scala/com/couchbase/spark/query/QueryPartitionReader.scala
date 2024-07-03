/*
 * Copyright (c) 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.spark.query

import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import com.couchbase.client.scala.query.{
  QueryMetaData,
  QueryMetrics,
  QueryScanConsistency,
  ReactiveQueryResult,
  QueryOptions => CouchbaseQueryOptions
}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.json.CouchbaseJsonUtils
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.reactivestreams.{Subscriber, Subscription}
import reactor.core.scala.publisher.{SFlux, SMono}
import reactor.core.scheduler.Schedulers

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.concurrent.duration.Duration

// Executes on the Spark worker.
class QueryPartitionReader(
    partition: QueryInputPartition,
    conf: CouchbaseConfig,
    readConfig: QueryReadConfig
) extends PartitionReader[InternalRow]
    with Logging {

  private val schema: StructType = partition.schema
  private val filters: Array[Filter] = partition.filters
  private val aggregations: Option[Aggregation] = partition.aggregations

  private val scopeName = readConfig.scope.getOrElse(DefaultConstants.DefaultScopeName)
  private val collectionName =
    readConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)

  private val parser       = CouchbaseJsonUtils.jsonParser(schema)
  private val createParser = CouchbaseJsonUtils.createParser()

  private val metaData = new AtomicReference[QueryMetaData]()

  private val groupByColumns = aggregations match {
    case Some(agg) => agg.groupByExpressions().map(n => n.references().head.fieldNames().head).toSeq
    case None      => Seq.empty
  }

  private val result: SMono[ReactiveQueryResult] = {
    if (
      scopeName.equals(DefaultConstants.DefaultScopeName) && collectionName.equals(
        DefaultConstants.DefaultCollectionName
      )
    ) {
      CouchbaseConnection(readConfig.connectionIdentifier)
        .cluster(conf)
        .reactive
        .query(buildQuery(), buildOptions())
    } else {
      CouchbaseConnection(readConfig.connectionIdentifier)
        .cluster(conf)
        .bucket(readConfig.bucket)
        .scope(scopeName)
        .reactive
        .query(buildQuery(), buildOptions())
    }
  }

  // SPARKC-178: Keep a backpressured flow of items between the ReactiveQueryResult and the PartitionedReader.
  private val DesiredItemsInQueue        = 30
  private val queue                      = new ConcurrentLinkedQueue[InternalRow]
  private val error                      = new AtomicReference[Throwable]()
  private val isComplete                 = new AtomicBoolean()
  private val subscription               = new AtomicReference[Subscription]()
  private val requestedButNotYetReceived = new AtomicInteger()

  // We intentionally don't change the scheduler, leaving it on the SDK's.  This is largely an IO-bound task so is
  // not expected to cause issues.
  result
    .flatMapMany(result => result.rowsAs[String](Passthrough.StringConvert))
    .subscribe(new Subscriber[String]() {
      override def onSubscribe(s: Subscription): Unit = {
        subscription.set(s)
      }

      override def onNext(r: String): Unit = {
        var row = r
        try {
          // Aggregates like MIN, MAX etc will end up as $1, $2 .. so they need to be replaced
          // with their original field names from the schema so that the JSON parser can pick
          // them up properly
          if (hasAggregateFields) {
            var idx = 1
            schema.fields.foreach(field => {
              if (!groupByColumns.contains(field.name)) {
                row = row.replace("$" + idx, field.name)
                idx = idx + 1
              }
            })
          }
          val x = parser.parse(row, createParser, UTF8String.fromString).toSeq
          if (x.size != 1) {
            throw new IllegalStateException(s"Expected 1 row, have ${x}")
          }
          queue.add(x.head)
        } catch {
          case e: Exception =>
            error.set(
              new IllegalStateException(
                s"Could not parse row $row based on provided schema $schema.",
                e
              )
            )
        }
      }

      override def onError(err: Throwable): Unit = error.set(err)

      override def onComplete(): Unit = isComplete.set(true)
    })

  result
    .flatMap(result => result.metaData)
    .subscribe(md => metaData.set(md))

  def buildQuery(): String = {
    var fields = schema.fields
      .map(f => f.name)
      .filter(f => !f.equals(readConfig.idFieldName))
      .map(f => maybeEscapeField(f))
    if (!hasAggregateFields) {
      fields = fields :+ s"META().id as `${readConfig.idFieldName}`"
    }

    var predicate       = readConfig.userFilter.map(p => s" WHERE $p").getOrElse("")
    val compiledFilters = QueryFilters.compile(filters)
    if (compiledFilters.nonEmpty && predicate.nonEmpty) {
      predicate = predicate + " AND " + compiledFilters
    } else if (compiledFilters.nonEmpty) {
      predicate = " WHERE " + compiledFilters
    }

    partition.bound match {
      case Some(value) =>
        if (predicate.contains("WHERE")) {
          predicate = predicate + s" AND (${value.whereClause})"
        }  else {
          predicate = predicate + s" WHERE (${value.whereClause})"
        }

      case None =>
    }

    val groupBy = if (hasAggregateGroupBy) {
      " GROUP BY " + aggregations.get
        .groupByExpressions()
        .map(n => s"`${n.references().head}`")
        .mkString(", ")
    } else {
      ""
    }

    val fieldsEncoded = fields.mkString(", ")

    val query =
      if (
        scopeName.equals(DefaultConstants.DefaultScopeName) && collectionName.equals(
          DefaultConstants.DefaultCollectionName
        )
      ) {
        s"select $fieldsEncoded from `${readConfig.bucket}`$predicate$groupBy"
      } else {
        s"select $fieldsEncoded from `$collectionName`$predicate$groupBy"
      }

    logDebug(s"Building and running N1QL query for `${readConfig.bucket}`.`${scopeName}`: $query")
    query
  }

  def hasAggregateFields: Boolean = {
    aggregations match {
      case Some(a) => !a.aggregateExpressions().isEmpty
      case None    => false
    }
  }

  def hasAggregateGroupBy: Boolean = {
    aggregations match {
      case Some(a) => !a.groupByExpressions().isEmpty
      case None    => false
    }
  }

  def maybeEscapeField(field: String): String = {
    if (
      field.startsWith("MAX")
      || field.startsWith("MIN")
      || field.startsWith("COUNT")
      || field.startsWith("SUM")
      || field.startsWith("AVG")
      || field.startsWith("`")
    ) {
      field
    } else {
      s"`$field`"
    }
  }

  def buildOptions(): CouchbaseQueryOptions = {
    var opts = CouchbaseQueryOptions().metrics(true)
    readConfig.scanConsistency match {
      case QueryOptions.NotBoundedScanConsistency =>
        opts = opts.scanConsistency(QueryScanConsistency.NotBounded)
      case QueryOptions.RequestPlusScanConsistency =>
        opts = opts.scanConsistency(QueryScanConsistency.RequestPlus())
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }
    readConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  override def next(): Boolean = {
    var isDone  = false
    var hasItem = false

    while (!isDone) {
      // Request more items if needed.
      val needToAdd = DesiredItemsInQueue - requestedButNotYetReceived.get
      if (needToAdd > 0) {
        requestedButNotYetReceived.addAndGet(needToAdd)
        subscription.get().request(needToAdd)
      }

      if (error.get != null) {
        throw error.get()
      }
      val next = queue.peek()
      if (next != null) {
        isDone = true
        hasItem = true
      } else {
        if (isComplete.get) {
          isDone = true
        }
      }
      Thread.sleep(1)
    }

    hasItem
  }

  override def get(): InternalRow = {
    val out = queue.poll()
    requestedButNotYetReceived.decrementAndGet()
    out
  }
  override def close(): Unit = {}

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val md = metaData.get()
    if (md != null) {
      md.metrics match {
        case Some(m) =>
          Array(
            new CustomTaskMetric {
              override def name(): String = "elapsedTimeMs"
              override def value(): Long  = m.elapsedTime.toMillis
            },
            new CustomTaskMetric {
              override def name(): String = "executionTimeMs"
              override def value(): Long  = m.executionTime.toMillis
            },
            new CustomTaskMetric {
              override def name(): String = "sortCount"
              override def value(): Long  = m.sortCount
            },
            new CustomTaskMetric {
              override def name(): String = "errorCount"
              override def value(): Long  = m.errorCount
            },
            new CustomTaskMetric {
              override def name(): String = "resultSize"
              override def value(): Long  = m.resultSize
            },
            new CustomTaskMetric {
              override def name(): String = "mutationCount"
              override def value(): Long  = m.mutationCount
            },
            new CustomTaskMetric {
              override def name(): String = "resultCount"
              override def value(): Long  = m.resultCount
            },
            new CustomTaskMetric {
              override def name(): String = "warningCount"
              override def value(): Long  = m.warningCount
            }
          )
        case None => Array()
      }
    } else {
      Array()
    }
  }
}
