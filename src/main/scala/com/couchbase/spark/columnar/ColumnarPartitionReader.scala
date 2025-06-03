/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.spark.columnar

import com.couchbase.client.scala.analytics.{AnalyticsMetrics, AnalyticsScanConsistency, AnalyticsOptions => CouchbaseAnalyticsOptions}
import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.spark.analytics.AnalyticsPartitionReader
import com.couchbase.spark.analytics.AnalyticsPartitionReader.{compileFilter, maybeEscapeField}
import com.couchbase.spark.columnar.ColumnarConstants.ColumnarEndpointIdx
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.CouchbaseJsonUtils
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.concurrent.duration.Duration
import scala.util.matching.Regex

class ColumnarPartitionReader(
                               schema: StructType,
                               conf: CouchbaseConfig,
                               readConfig: ColumnarReadConfig,
                               filters: Array[Filter],
                               aggregations: Option[Aggregation],
                               limit: Option[Int]
) extends PartitionReader[InternalRow]
    with Logging {

  private val parser       = CouchbaseJsonUtils.jsonParser(schema)
  private val createParser = CouchbaseJsonUtils.createParser()

  private var resultMetrics: Option[AnalyticsMetrics] = None

  private val groupByColumns = aggregations match {
    case Some(agg) => agg.groupByExpressions().map(n => n.references().head.fieldNames().head).toSeq
    case None      => Seq.empty
  }

  private lazy val result = {
    logInfo(s"Running Columnar query ${buildColumnarQuery()}")
    CouchbaseConnection(readConfig.connectionIdentifier)
      .cluster(conf)
      .analyticsQuery(buildColumnarQuery(), buildOptions())
  }

  private lazy val rows = result
    .flatMap(result => {
      resultMetrics = Some(result.metaData.metrics)
      result.rowsAs[String](Passthrough.StringConvert)
    })
    .get
    .flatMap(r => {
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

        val out = parser.parse(row, createParser, UTF8String.fromString)
        out
      } catch {
        case e: Exception =>
          throw new IllegalStateException(
            s"Could not parse row $row based on provided schema $schema.",
            e
          )
      }
    })

  private lazy val rowIterator       = rows.iterator
  protected var lastRow: InternalRow = InternalRow()

  override def next(): Boolean = if (rowIterator.hasNext) {
    lastRow = rowIterator.next()
    true
  } else {
    false
  }

  override def get(): InternalRow = lastRow
  override def close(): Unit      = {}

  def buildColumnarQuery(): String = {
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
      case None => ""
    }

    val fieldsEncoded = fields.mkString(", ")
    val query         = s"select $fieldsEncoded from `${readConfig.collection}`$predicate$groupBy$limitStr"

    query
  }

  def buildOptions(): CouchbaseAnalyticsOptions = {
    var opts = CouchbaseAnalyticsOptions().endpointIdx(ColumnarEndpointIdx)
    readConfig.scanConsistency match {
      case ColumnarOptions.NotBoundedScanConsistency =>
        opts = opts.scanConsistency(AnalyticsScanConsistency.NotBounded)
      case ColumnarOptions.RequestPlusScanConsistency =>
        opts = opts.scanConsistency(AnalyticsScanConsistency.RequestPlus)
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }
    readConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts = opts.raw(Map("query_context" -> ColumnarQueryContext.queryContext(readConfig.database, readConfig.scope)))
    opts
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

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    AnalyticsPartitionReader.currentMetricsValues(resultMetrics)
  }
}
