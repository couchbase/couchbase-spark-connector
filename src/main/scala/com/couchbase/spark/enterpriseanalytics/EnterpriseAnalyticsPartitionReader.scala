/*
 * Copyright (c) 2025 Couchbase, Inc.
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

package com.couchbase.spark.enterpriseanalytics

import com.couchbase.analytics.client.java.{QueryOptions, Row}
import com.couchbase.spark.analytics.AnalyticsPartitionReader.{compileFilter, maybeEscapeField}
import com.couchbase.spark.enterpriseanalytics.config.{
  EnterpriseAnalyticsConfig,
  EnterpriseAnalyticsConnection
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.CouchbaseJsonUtils
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import scala.concurrent.duration.Duration

class EnterpriseAnalyticsPartitionReader(
    schema: StructType,
    conf: EnterpriseAnalyticsConfig,
    readConfig: EnterpriseAnalyticsReadConfig,
    filters: Array[Filter],
    aggregations: Option[Aggregation],
    limit: Option[Int]
) extends PartitionReader[InternalRow]
    with Logging {

  private val parser       = CouchbaseJsonUtils.jsonParser(schema)
  private val createParser = CouchbaseJsonUtils.createParser()

  // SPARKC-178: Keep a backpressured flow of items between Spark and SDK.
  // In this case the SDK is push based.
  private val MaxQueueSize = 100
  private val queue        = new LinkedBlockingQueue[InternalRow](MaxQueueSize)
  private val error        = new AtomicReference[Throwable]()
  private val isComplete   = new AtomicBoolean()
  private val started      = System.nanoTime
  private val query        = buildEnterpriseAnalyticsQuery()
  private val hyperVerbose =
    System.getProperty("spark.couchbase.enterpriseanalytics.hyperVerbose", "false").toBoolean

  // Execute the query in a separate thread to avoid blocking the constructor
  {
    val executor = Executors.newSingleThreadExecutor()
    executor.submit(new Runnable {
      override def run(): Unit = {
        try {
          logInfo(s"Running Enterprise Analytics query ${query}")

          val cluster = EnterpriseAnalyticsConnection().cluster(conf)

          val md = cluster.executeStreamingQuery(
            query,
            (row) => processRow(row),
            (queryOpts: QueryOptions) => {
              readConfig.timeout match {
                case Some(value) =>
                  queryOpts.timeout(java.time.Duration.ofNanos(Duration(value).toNanos))
                case _ =>
              }
            }
          )
          logInfo(s"Finished Enterprise Analytics query ${query} in ${Duration
              .fromNanos(System.nanoTime() - started)}, metrics from server: $md")
          isComplete.set(true)
        } catch {
          case e: Exception =>
            logError(
              s"Error running Enterprise Analytics query ${query} after ${Duration
                  .fromNanos(System.nanoTime() - started)}",
              e
            )
            error.set(e)
        } finally {
          executor.shutdown()
        }
      }
    })
  }

  private def processRow(rowRaw: Row): Unit = {
    try {
      if (hyperVerbose)
        logInfo(s"processRow() called, rowRaw: $rowRaw, queue size: ${queue.size()}")

      // We have to convert to string to do the aggregate replacement
      var row = new String(rowRaw.bytes(), "UTF-8")

      // Aggregates like MIN, MAX etc will end up as $1, $2 .. so they need to be replaced
      // with their original field names from the schema so that the JSON parser can pick
      // them up properly
      if (hasAggregateFields) {
        val groupByColumns = aggregations match {
          case Some(agg) =>
            agg.groupByExpressions().map(n => n.references().head.fieldNames().head).toSeq
          case None => Seq.empty
        }

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
      // The SDK is push-based.  If we block inside the row consumer (because the queue is full, say), then this automatically
      // backpressures the SDK - e.g. it will stop fetching data from the server also.
      queue.put(x.head)
    } catch {
      case e: Exception =>
        error.set(
          new IllegalStateException(
            s"Could not parse row $rowRaw based on provided schema $schema.",
            e
          )
        )
    }
  }

  // The Spark interface asking if there's another row available
  override def next(): Boolean = {
    if (hyperVerbose) logInfo(s"next() called, queue size: ${queue.size()}")

    var isDone  = false
    var hasItem = false

    while (!isDone) {
      if (error.get != null) {
        logError(s"Reporting error to Spark")
        throw error.get()
      }

      if (hyperVerbose) logInfo(s"next() waiting for queue item, queue size: ${queue.size()}")

      val next = queue.peek()
      if (next != null) {
        isDone = true
        hasItem = true
      } else if (isComplete.get) {
        logInfo(s"Reported complete to Spark, items remaining in queue: ${queue.size()}")
        isDone = true
      } else {
        Thread.sleep(1)
      }
    }

    if (hyperVerbose) logInfo(s"next() returning hasItem: $hasItem")

    hasItem
  }

  // The Spark interface fetching the next row
  override def get(): InternalRow = {
    if (hyperVerbose) logInfo(s"get() called, queue size: ${queue.size()}")
    val item = queue.poll()
    if (hyperVerbose) logInfo(s"get() returning item: $item")
    item
  }

  override def close(): Unit = {}

  def buildEnterpriseAnalyticsQuery(): String = {
    val fields = schema.fields
      .map(f => f.name)
      .map(f => maybeEscapeField(f))

    var predicate       = readConfig.userFilter.map(p => s" WHERE $p").getOrElse("")
    val compiledFilters = compileFilter(filters)
    if (compiledFilters.nonEmpty && predicate.nonEmpty) {
      predicate = predicate + " AND " + compiledFilters
    } else if (compiledFilters.nonEmpty) {
      predicate = " WHERE " + compiledFilters
    }

    val groupBy = if (hasAggregateGroupBy) {
      " GROUP BY " + aggregations.get
        .groupByExpressions()
        .map(n => s"`${n.references().head}`")
        .mkString(", ")
    } else {
      ""
    }

    val limitStr = limit match {
      case Some(value) => " LIMIT " + value
      case None        => ""
    }

    val fieldsEncoded = fields.mkString(", ")
    val query =
      s"select $fieldsEncoded from ${readConfig.escapedQualifiedCollection}$predicate$groupBy$limitStr"

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
}
