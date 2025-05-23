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

import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class QueryScanBuilder(schema: StructType, readConfig: QueryReadConfig)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates
    with SupportsPushDownLimit {

  private var finalSchema                       = schema
  private var pushedFilter                      = Array.empty[Filter]
  private var aggregations: Option[Aggregation] = None
  private var limit: Option[Int]                = None

  override def build(): Scan = {
    new QueryScan(finalSchema, readConfig, pushedFilter, aggregations, limit)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushedFilter = filters
    Array.empty[Filter]
  }

  override def pushedFilters(): Array[Filter] = pushedFilter
  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (requiredSchema != null && requiredSchema.nonEmpty) {
      finalSchema = requiredSchema
    }
  }

  override def pushAggregation(agg: Aggregation): Boolean = {
    if (!readConfig.pushDownAggregate) {
      return false
    }

    val aggregateFuncs = QueryAggregations.convertAggregateExpressions(agg, schema)
    if (aggregateFuncs.isEmpty) {
      return false
    }

    finalSchema = if (agg.groupByExpressions().isEmpty) {
      StructType(aggregateFuncs)
    } else {
      val aggregations = QueryAggregations.convertGroupByExpression(agg, schema)
      if (aggregations.isEmpty) {
        return false
      } else {
        StructType(aggregations ++ aggregateFuncs)
      }
    }

    aggregations = Some(agg)
    true
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    readConfig.partition match {
      case Some(_) =>
        // In partitioning mode, push-down aggregation is not supported - see SPARKC-206
        false
      case None => QueryAggregations.supportsCompleteAggPushdown(aggregation)
    }
  }

  override def pushLimit(limit: Int): Boolean = {
    this.limit = Some(limit)
    true
  }
}
