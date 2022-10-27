/*
 * Copyright (c) 2022 Couchbase, Inc.
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
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.types.{
  DataType,
  DecimalType,
  DoubleType,
  FloatType,
  LongType,
  StructField,
  StructType
}

/** Helper object to deal with aggregations in N1QL/SQL++. */
object QueryAggregations extends Logging {

  /** Converts a spark aggregation into a sequence of fields. If empty the aggregation is not
    * supported or something else went wrong.
    *
    * @param agg
    *   the aggregation to convert.
    * @return
    *   a non-empty sequence if supported and present.
    */
  def convertAggregateExpressions(agg: Aggregation, schema: StructType): Seq[StructField] = {
    agg.aggregateExpressions
      .map({
        case min: Min =>
          if (min.column.references.length != 1) return Seq.empty
          val fieldName = min.column.references.head
          val original  = structFieldForName(fieldName.fieldNames.head, schema).get
          // MIN uses the original input type as the output type
          StructField(s"MIN(`$fieldName`)", original.dataType, original.nullable, original.metadata)
        case max: Max =>
          if (max.column.references.length != 1) return Seq.empty
          val fieldName = max.column.references.head
          val original  = structFieldForName(fieldName.fieldNames.head, schema).get
          // MAX uses the original input type as the output type
          StructField(s"MAX(`$fieldName`)", original.dataType, original.nullable, original.metadata)
        case count: Count =>
          if (count.column.references.length != 1) return Seq.empty
          val fieldName = count.column.references.head
          val original  = structFieldForName(fieldName.fieldNames.head, schema).get
          val distinct  = if (count.isDistinct) "DISTINCT " else ""
          // COUNT always returns the LongType, regardless of the input type
          StructField(
            s"COUNT($distinct`$fieldName`)",
            LongType,
            original.nullable,
            original.metadata
          )
        case sum: Sum =>
          if (sum.column.references.length != 1) return Seq.empty
          val fieldName = sum.column.references.head
          val original  = structFieldForName(fieldName.fieldNames.head, schema).get
          val distinct  = if (sum.isDistinct) "DISTINCT " else ""
          // If the original input type is fractional use a big fractional type, otherwise use a big integer
          // type to fit in as much as possible on the sum.
          val dataType = if (isFractional(original.dataType)) {
            DoubleType
          } else {
            LongType
          }
          StructField(
            s"SUM($distinct`$fieldName`)",
            dataType,
            original.nullable,
            original.metadata
          )
        case avg: Avg =>
          if (avg.column.references.length != 1) return Seq.empty
          val fieldName = avg.column.references.head
          val original  = structFieldForName(fieldName.fieldNames.head, schema).get
          val distinct  = if (avg.isDistinct) "DISTINCT " else ""
          // AVG is always of FloatType, even if the original field is an int/long type
          StructField(
            s"AVG($distinct`$fieldName`)",
            FloatType,
            original.nullable,
            original.metadata
          )
        case _: CountStar =>
          StructField(s"COUNT(*)", LongType)
        case _ => return Seq.empty
      })
      .toSeq
  }

  def supportsCompleteAggPushdown(agg: Aggregation): Boolean = {
    agg.aggregateExpressions().foreach {
      // These are all fine and supported...
      case _: Min | _: Max | _: Avg | _: Sum | _: Count | _: CountStar =>
      // For all the others report back to spark that complete agg pushdown is not supported
      case _ => return false
    }
    true
  }

  def convertGroupByExpression(agg: Aggregation, schema: StructType): Seq[StructField] = {
    agg.groupByExpressions.map { col =>
      if (col.references.length != 1) return Seq.empty
      val fieldName = col.references.head
      val original  = structFieldForName(fieldName.fieldNames.head, schema).get
      StructField(
        fieldName.fieldNames.head,
        original.dataType,
        original.nullable,
        original.metadata
      )
    }.toSeq
  }

  private def structFieldForName(name: String, schema: StructType): Option[StructField] = {
    schema.fields.find(f => f.name.equals(name))
  }

  private def isFractional(dataType: DataType): Boolean = {
    dataType match {
      case _: DecimalType | FloatType | DoubleType => true
      case _                                       => false
    }
  }

}
