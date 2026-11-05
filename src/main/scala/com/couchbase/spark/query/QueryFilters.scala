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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.{
  AlwaysFalse,
  AlwaysTrue,
  And,
  EqualNullSafe,
  EqualTo,
  Filter,
  GreaterThan,
  GreaterThanOrEqual,
  In,
  IsNotNull,
  IsNull,
  LessThan,
  LessThanOrEqual,
  Not,
  Or,
  StringContains,
  StringEndsWith,
  StringStartsWith
}

import scala.util.matching.Regex

object QueryFilters extends Logging {

  /** Splits filters into those that can be compiled to N1QL, and so pushed down, and those that
    * cannot.
    *
    * The unsupported filters must be returned to Spark from `SupportsPushDownFilters.pushFilters`,
    * so that Spark evaluates them itself after the scan.
    *
    * @return
    *   (supported, unsupported)
    */
  def partitionSupported(filters: Array[Filter]): (Array[Filter], Array[Filter]) = {
    val (supported, unsupported) = filters.partition(f => filterToExpression(f).isDefined)
    unsupported.foreach(f => logInfo(s"Filter cannot be pushed down, Spark will evaluate it: $f"))
    (supported, unsupported)
  }

  /** Transform the filters into a N1QL where clause.
    *
    * @param filters
    *   the filters to transform, which must all be supported (see [[partitionSupported]])
    * @return
    *   the transformed raw N1QL clause
    */
  def compile(filters: Array[Filter]): String =
    filters
      .map(f =>
        filterToExpression(f).getOrElse(
          throw new IllegalArgumentException(s"Filter cannot be pushed down: $f")
        )
      )
      .mkString(" AND")

  /** Turns a filter into a N1QL expression.
    *
    * @param filter
    *   the filter to convert
    * @return
    *   the resulting expression, or None if the filter cannot be expressed in N1QL
    */
  def filterToExpression(filter: Filter): Option[String] = {
    filter match {
      case AlwaysFalse() => Some(" FALSE")
      case AlwaysTrue()  => Some(" TRUE")
      case And(left, right) =>
        for (l <- filterToExpression(left); r <- filterToExpression(right)) yield s" ($l AND $r)"
      case EqualNullSafe(attr, value) =>
        Some(
          s" (NOT (${attrToFilter(attr)} != " + valueToFilter(value) + s" OR ${attrToFilter(attr)} IS NULL OR " + valueToFilter(
            value
          ) + s" IS NULL) OR (${attrToFilter(attr)} IS NULL AND " + valueToFilter(
            value
          ) + " IS NULL))"
        )
      case EqualTo(attr, value)     => Some(s" ${attrToFilter(attr)} = " + valueToFilter(value))
      case GreaterThan(attr, value) => Some(s" ${attrToFilter(attr)} > " + valueToFilter(value))
      case GreaterThanOrEqual(attr, value) =>
        Some(s" ${attrToFilter(attr)} >= " + valueToFilter(value))
      case In(attr, values) =>
        val encoded = values.map(valueToFilter).mkString(",")
        Some(s" `$attr` IN [$encoded]")
      case IsNotNull(attr)       => Some(s" ${attrToFilter(attr)} IS NOT NULL")
      case IsNull(attr)          => Some(s" ${attrToFilter(attr)} IS NULL")
      case LessThan(attr, value) => Some(s" ${attrToFilter(attr)} < " + valueToFilter(value))
      case LessThanOrEqual(attr, value) =>
        Some(s" ${attrToFilter(attr)} <= " + valueToFilter(value))
      case Not(f) =>
        filterToExpression(f).map(v => s" NOT ($v)")
      case Or(left, right) =>
        for (l <- filterToExpression(left); r <- filterToExpression(right)) yield s" ($l OR $r)"
      case StringContains(attr, value) => Some(s" CONTAINS(${attrToFilter(attr)}, '$value')")
      case StringEndsWith(attr, value) =>
        Some(s" ${attrToFilter(attr)} LIKE '%" + escapeForLike(value) + "'")
      case StringStartsWith(attr, value) =>
        Some(s" ${attrToFilter(attr)} LIKE '" + escapeForLike(value) + "%'")
      // Anything else, e.g. the CollatedFilters that Spark 4 produces for collated string columns.
      case _ => None
    }
  }

  def escapeForLike(value: String): String =
    value.replaceAll("\\.", "\\\\.").replaceAll("\\*", "\\\\*")

  def valueToFilter(value: Any): String = value match {
    case v: String => s"'$v'"
    case v         => s"$v"
  }

  val VerbatimRegex: Regex = """'(.*)'""".r

  def attrToFilter(attr: String): String = attr match {
    case VerbatimRegex(innerAttr) => innerAttr
    case v                        => v.split('.').map(elem => s"`$elem`").mkString(".")
  }

}
