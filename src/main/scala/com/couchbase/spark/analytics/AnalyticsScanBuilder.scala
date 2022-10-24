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

import com.couchbase.spark.query.QueryAggregations
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

class AnalyticsScanBuilder(schema: StructType, readConfig: AnalyticsReadConfig)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates {

  private var finalSchema = schema
  private var pushedFilter = Array.empty[Filter]
  private var aggregations: Option[Aggregation] = None

  override def build(): Scan = {
    new AnalyticsScan(finalSchema, readConfig, pushedFilter, aggregations)
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

  private def structFieldForName(name: String): Option[StructField] = {
    schema.fields.find(f => f.name.equals(name))
  }
}
