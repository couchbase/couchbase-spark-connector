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

import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Count, CountStar, Max, Min, Sum}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class QueryScanBuilder(schema: StructType, readConfig: QueryReadConfig)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownAggregates {

  private var finalSchema = schema
  private var pushedFilter = Array.empty[Filter]
  private var aggregations: Option[Aggregation] = None

  override def build(): Scan = {
    new QueryScan(finalSchema, readConfig, pushedFilter, aggregations)
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

    val aggregateFuncs = agg.aggregateExpressions().map({
       case min: Min =>
         if (min.column.fieldNames.length != 1) return false
         val fieldName = min.column.fieldNames.head
         val original = structFieldForName(fieldName).get
         StructField(s"MIN(`$fieldName`)", original.dataType, original.nullable, original.metadata)
       case max: Max =>
         if (max.column.fieldNames.length != 1) return false
         val fieldName = max.column.fieldNames.head
         val original = structFieldForName(fieldName).get
         StructField(s"MAX(`$fieldName`)", original.dataType, original.nullable, original.metadata)
       case count: Count =>
         if (count.column.fieldNames.length != 1) return false
         val fieldName = count.column.fieldNames.head
         val original = structFieldForName(fieldName).get
         val distinct = if (count.isDistinct) "DISTINCT " else ""
         StructField(s"COUNT($distinct`$fieldName`)", original.dataType, original.nullable, original.metadata)
       case sum: Sum =>
         if (sum.column.fieldNames.length != 1) return false
         val fieldName = sum.column.fieldNames.head
         val original = structFieldForName(fieldName).get
         val distinct = if (sum.isDistinct) "DISTINCT " else ""
         StructField(s"SUM($distinct`$fieldName`)", original.dataType, original.nullable, original.metadata)
       case _: CountStar =>
         StructField(s"COUNT(*)", LongType)
       case _ => return false
    }).toSeq

    if (aggregateFuncs.isEmpty) {
      return false
    }

    val groupByCols = agg.groupByColumns.map { col =>
      if (col.fieldNames.length != 1) return false
      val fieldName = col.fieldNames.head
      val original = structFieldForName(fieldName).get
      StructField(fieldName, original.dataType, original.nullable, original.metadata)
    }.toSeq

    val allFields = groupByCols ++ aggregateFuncs
    finalSchema = StructType(allFields)
    aggregations = Some(agg)
    true
  }

  private def structFieldForName(name: String): Option[StructField] = {
    schema.fields.find(f => f.name.equals(name))
  }

}
