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

import com.couchbase.spark.kv.KeyValueOptions
import com.couchbase.spark.util.{SparkOperationalSimpleTest, TestNameUtil}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeAll, Test}

class QueryAggregatePushDownIntegrationTest extends SparkOperationalSimpleTest {
  override def testName: String = TestNameUtil.testName

  @BeforeAll
  def writeTestData(): Unit = {
    val session = spark
    import session.implicits._
    // Group-by values deliberately contain the "$1" placeholders that the query service gives unnamed
    // aggregate results.  "big" values are chosen so that a single-precision AVG loses digits.
    val docs = Seq(
      """{"__META_ID": "a1", "type": "agg", "category": "$1 off", "price": 10, "big": 123456789}""",
      """{"__META_ID": "a2", "type": "agg", "category": "$1 off", "price": 20, "big": 123456790}""",
      """{"__META_ID": "a3", "type": "agg", "category": "$2 off", "price": 30, "big": 1}"""
    )
    spark.read
      .json(docs.toDS())
      .write
      .format("couchbase.kv")
      .mode(SaveMode.Overwrite)
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .save()
  }

  private def readAggs(): DataFrame = {
    val df = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, testResources.bucketName)
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      // The collection also holds the shared airport sample data.
      .option(QueryOptions.Filter, "type = 'agg'")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()
    df.createOrReplaceTempView("aggs")
    df
  }

  private def assertPushedDown(df: DataFrame, expected: String*): Unit = {
    val scans = df.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation => p.toString() }
    assertEquals(1, scans.size)
    expected.foreach(e => assertTrue(scans.head.contains(e), s"Expected '$e' in pushed scan ${scans.head}"))
  }

  @Test
  def groupByValuesContainingAggregatePlaceholdersAreNotModified(): Unit = {
    readAggs()
    val result = spark.sql("select category, min(price) as mn from aggs group by category")
    assertPushedDown(result, "MIN(`price`)")

    val rows = result.collect().map(r => r.getAs[String]("category") -> r.getAs[Long]("mn")).toMap
    assertEquals(Map("$1 off" -> 10L, "$2 off" -> 30L), rows)
  }

  @Test
  def tenOrMoreAggregates(): Unit = {
    readAggs()
    // The 10th aggregate is "$10" in the query result, which a "$1" replacement would corrupt.
    val result = spark.sql(
      """select min(price), max(price), sum(price), count(price), avg(price),
        |       min(big), max(big), sum(big), count(big), max(category)
        |from aggs""".stripMargin
    )
    assertPushedDown(result, "MAX(`category`)")

    val row = result.collect().head
    assertEquals(
      Row(10L, 30L, 60L, 3L, 20.0, 1L, 123456790L, 246913580L, 3L, "$2 off"),
      row
    )
  }

  @Test
  def avgKeepsDoublePrecision(): Unit = {
    readAggs()
    val result = spark.sql("select avg(big) as a from aggs")
    assertPushedDown(result, "AVG(`big`)")

    // (123456789 + 123456790 + 1) / 3.  As a 32-bit float this would be 82304528.
    assertEquals(82304526.666666667, result.collect().head.getAs[Double]("a"), 1e-6)
  }

  @Test
  def avgWithGroupBy(): Unit = {
    readAggs()
    val result = spark.sql("select category, avg(big) as a from aggs group by category")
    assertPushedDown(result, "AVG(`big`)")

    val rows = result.collect().map(r => r.getAs[String]("category") -> r.getAs[Double]("a")).toMap
    assertEquals(123456789.5, rows("$1 off"), 1e-6)
    assertEquals(1.0, rows("$2 off"), 1e-6)
  }
}
