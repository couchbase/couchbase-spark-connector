/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation, Avg, Max, Min}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class QueryAggregationsTest {
  private val schema = StructType(
    Seq(
      StructField("category", StringType),
      StructField("price", LongType),
      StructField("big", LongType)
    )
  )

  private def aggregation(aggs: Seq[AggregateFunc], groupBy: Seq[String]): Aggregation =
    new Aggregation(aggs.toArray, groupBy.map(g => Expressions.column(g): Expression).toArray)

  @Test
  def avgIsDouble(): Unit = {
    val agg    = aggregation(Seq(new Avg(Expressions.column("big"), false)), Seq.empty)
    val fields = QueryAggregations.convertAggregateExpressions(agg, schema)
    assertEquals(Seq(DoubleType), fields.map(_.dataType))
  }

  @Test
  def aggregatesAreAliasedAndGroupByColumnsAreNot(): Unit = {
    val agg = aggregation(
      Seq(new Min(Expressions.column("price")), new Max(Expressions.column("big"))),
      Seq("category")
    )
    // Mirrors the schema QueryScanBuilder.pushAggregation produces: group-by columns, then aggregates.
    val readSchema = StructType(
      QueryAggregations.convertGroupByExpression(agg, schema) ++
        QueryAggregations.convertAggregateExpressions(agg, schema)
    )
    assertEquals(Seq("category", "MIN(`price`)", "MAX(`big`)"), readSchema.fieldNames.toSeq)

    val aliases = QueryAggregations.aggregateAliases(readSchema, Some(agg))
    assertEquals(Seq(None, Some("__cb_agg_1"), Some("__cb_agg_2")), aliases)

    val result = QueryAggregations.resultSchema(readSchema, Some(agg))
    assertEquals(Seq("category", "__cb_agg_1", "__cb_agg_2"), result.fieldNames.toSeq)
    assertEquals(readSchema.fields.map(_.dataType).toSeq, result.fields.map(_.dataType).toSeq)

    assertEquals("MIN(`price`) AS `__cb_agg_1`", QueryAggregations.selectTerm("MIN(`price`)", aliases(1)))
    assertEquals("`category`", QueryAggregations.selectTerm("`category`", aliases.head))
  }

  @Test
  def tenOrMoreAggregatesGetDistinctAliases(): Unit = {
    val aggs        = (1 to 11).map(_ => new Min(Expressions.column("price")))
    val agg         = aggregation(aggs, Seq.empty)
    val readSchema  = StructType((1 to 11).map(i => StructField(s"agg$i", LongType)))
    val aliases     = QueryAggregations.aggregateAliases(readSchema, Some(agg)).flatten
    assertEquals((1 to 11).map(i => s"__cb_agg_$i"), aliases)
  }

  @Test
  def noAggregatesMeansNoAliases(): Unit = {
    assertEquals(Seq(None, None, None), QueryAggregations.aggregateAliases(schema, None))
    assertEquals(schema, QueryAggregations.resultSchema(schema, None))

    // Group-by only, e.g. a pushed-down DISTINCT.
    val groupByOnly = aggregation(Seq.empty, Seq("category"))
    val readSchema  = StructType(Seq(StructField("category", StringType)))
    assertEquals(Seq(None), QueryAggregations.aggregateAliases(readSchema, Some(groupByOnly)))
  }
}
