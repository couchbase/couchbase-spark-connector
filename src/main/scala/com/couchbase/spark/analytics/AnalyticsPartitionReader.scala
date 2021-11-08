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

package com.couchbase.spark.analytics

import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CouchbaseJsonUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import com.couchbase.client.scala.analytics.{AnalyticsScanConsistency, AnalyticsOptions => CouchbaseAnalyticsOptions}

import scala.concurrent.duration.Duration
import scala.util.matching.Regex

class AnalyticsPartitionReader(schema: StructType, conf: CouchbaseConfig, readConfig: AnalyticsReadConfig, filters: Array[Filter])
  extends PartitionReader[InternalRow]
    with Logging {

  private val parser = CouchbaseJsonUtils.jsonParser(schema)
  private val createParser = CouchbaseJsonUtils.createParser()

  private lazy val result = {
    if (readConfig.bucket.isEmpty || readConfig.scope.isEmpty) {
      CouchbaseConnection().cluster(conf).analyticsQuery(buildAnalyticsQuery(), buildOptions())
    } else {
      CouchbaseConnection().cluster(conf).bucket(readConfig.bucket.get).scope(readConfig.scope.get).analyticsQuery(buildAnalyticsQuery(), buildOptions())
    }
  }

  private lazy val rows = result
    .flatMap(result => result.rowsAs[String](Passthrough.StringConvert))
    .get
    .flatMap(row => {
      try {
        parser.parse(row, createParser, UTF8String.fromString)
      } catch {
        case e: Exception =>
          throw new IllegalStateException(s"Could not parse row $row based on provided schema $schema.", e)
      }
    })

  private lazy val rowIterator = rows.iterator
  protected var lastRow: InternalRow = InternalRow()

  override def next(): Boolean = if (rowIterator.hasNext) {
    lastRow = rowIterator.next()
    true
  } else {
    false
  }

  override def get(): InternalRow = lastRow
  override def close(): Unit = {}

  def buildAnalyticsQuery(): String = {
    val fields = schema
      .fields
      .map(f => f.name)
      .filter(f => !f.equals(readConfig.idFieldName))
      .map(f => s"`$f`")
      .mkString(", ")

    var predicate = readConfig.userFilter.map(p => s" WHERE $p").getOrElse("")
    val compiledFilters = compileFilter(filters)
    if (compiledFilters.nonEmpty && predicate.nonEmpty) {
      predicate = predicate + " AND " + compiledFilters
    } else if (compiledFilters.nonEmpty) {
      predicate = " WHERE " + compiledFilters
    }

    val query = s"select META().id as ${readConfig.idFieldName}, $fields from `${readConfig.dataset}`$predicate"

    logDebug(s"Building and running Analytics query $query")
    query
  }

  def buildOptions(): CouchbaseAnalyticsOptions = {
    var opts = CouchbaseAnalyticsOptions()
    readConfig.scanConsistency match {
      case AnalyticsOptions.NotBoundedScanConsistency => opts = opts.scanConsistency(AnalyticsScanConsistency.NotBounded)
      case AnalyticsOptions.RequestPlusScanConsistency => opts = opts.scanConsistency(AnalyticsScanConsistency.RequestPlus)
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }
    readConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }


  /** Transform the filters into a analytics sql++ where clause.
   *
   * @todo In, And, Or, Not filters including recursion
   * @param filters the filters to transform
   * @return the transformed raw analytics sql++ clause
   */
  def compileFilter(filters: Array[Filter]): String = {
    if (filters.isEmpty) {
      return ""
    }

    val filter = new StringBuilder()
    var i = 0

    filters.foreach(f => {
      try {
        val encoded = filterToExpression(f)
        if (i > 0) {
          filter.append(" AND")
        }
        filter.append(encoded)
        i = i + 1
      } catch {
        case _: Exception => logInfo("Ignoring unsupported filter: " + f)
      }
    })

    filter.toString()
  }

  /**
   * Turns a filter into a sql++ expression.
   *
   * @param filter the filter to convert
   * @return the resulting expression
   */
  def filterToExpression(filter: Filter): String = {
    filter match {
      case AlwaysFalse() => " FALSE"
      case AlwaysTrue() => " TRUE"
      case And(left, right) =>
        val l = filterToExpression(left)
        val r = filterToExpression(right)
        s" ($l AND $r)"
      case EqualNullSafe(attr, value) => s" (NOT (${attrToFilter(attr)} != "+valueToFilter(value)+s" OR ${attrToFilter(attr)} IS NULL OR "+valueToFilter(value)+s" IS NULL) OR (${attrToFilter(attr)} IS NULL AND " + valueToFilter(value) + " IS NULL))"
      case EqualTo(attr, value) => s" ${attrToFilter(attr)} = " + valueToFilter(value)
      case GreaterThan(attr, value) => s" ${attrToFilter(attr)} > " + valueToFilter(value)
      case GreaterThanOrEqual(attr, value) => s" ${attrToFilter(attr)} >= " + valueToFilter(value)
      case In(attr, values) =>
        val encoded = values.map(valueToFilter).mkString(",")
        s" `$attr` IN [$encoded]"
      case IsNotNull(attr) => s" ${attrToFilter(attr)} IS NOT NULL"
      case IsNull(attr) => s" ${attrToFilter(attr)} IS NULL"
      case LessThan(attr, value) => s" ${attrToFilter(attr)} < " + valueToFilter(value)
      case LessThanOrEqual(attr, value) => s" ${attrToFilter(attr)} <= " + valueToFilter(value)
      case Not(f) =>
        val v = filterToExpression(f)
        s" NOT ($v)"
      case Or(left, right) =>
        val l = filterToExpression(left)
        val r = filterToExpression(right)
        s" ($l OR $r)"
      case StringContains(attr, value) => s" CONTAINS(${attrToFilter(attr)}, '$value')"
      case StringEndsWith(attr, value) =>
        s" ${attrToFilter(attr)} LIKE '%" + escapeForLike(value) + "'"
      case StringStartsWith(attr, value) =>
        s" ${attrToFilter(attr)} LIKE '" + escapeForLike(value) + "%'"
    }
  }

  def escapeForLike(value: String): String =
    value.replaceAll("\\.", "\\\\.").replaceAll("\\*", "\\\\*")

  def valueToFilter(value: Any): String = value match {
    case v: String => s"'$v'"
    case v => s"$v"
  }

  val VerbatimRegex: Regex = """'(.*)'""".r

  def attrToFilter(attr: String): String = attr match {
    case VerbatimRegex(innerAttr) => innerAttr
    case v => v.split('.').map(elem => s"`$elem`").mkString(".")
  }

}
