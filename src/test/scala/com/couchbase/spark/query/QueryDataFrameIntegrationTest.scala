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

import com.couchbase.spark.util.{SparkOperationalSimpleTest, SparkOperationalTest, TestNameUtil}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class QueryDataFrameIntegrationTest extends SparkOperationalSimpleTest {
  override def testName: String = TestNameUtil.testName

  @Test
  def testReadDocumentsWithFilter(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airport'")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertEquals("airport", row.getAs[String]("type"))
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

  @Test
  def testReadDocumentsWithLimit(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airport'")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()
      .limit(2)

    assertEquals(2, airports.count)
  }

  @Test
  def testChangeIdFieldName(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airport'")
      .option(QueryOptions.IdFieldName, "myIdFieldName")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.foreach(row => {
      assertEquals("airport", row.getAs[String]("type"))
      assertThrows(classOf[IllegalArgumentException], () => row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("myIdFieldName"))
    })
  }

  @Test
  def testPushDownAggregationWithoutGroupBy(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airport'")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.createOrReplaceTempView("airports")

    val aggregates = spark.sql(
      "select max(elevation) as el, min(runways) as run from airports"
    )

    aggregates.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation =>
      println(p.toString())
      assertTrue(p.toString().contains("MAX(`elevation`)"))
      assertTrue(p.toString().contains("MIN(`runways`)"))
    }

    assertEquals(204, aggregates.first().getAs[Long]("el"))
    assertEquals(2, aggregates.first().getAs[Long]("run"))
  }

  @Test
  def testPushDownAggregationWithGroupBy(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airport'")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.createOrReplaceTempView("airports")

    val aggregates = spark.sql(
      "select max(elevation) as el, min(runways) as run, country from airports group by country"
    )

    aggregates.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation =>
      assertTrue(p.toString().contains("country"))
      assertTrue(p.toString().contains("MAX(`elevation`)"))
      assertTrue(p.toString().contains("MIN(`runways`)"))
    }

    assertEquals(3, aggregates.count())
    assertEquals(183, aggregates.where("country = 'Austria'").first().getAs[Long]("el"))
    assertEquals(4, aggregates.where("country = 'Germany'").first().getAs[Long]("run"))
  }

  @Test
  def testPushDownAvgAggregate(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airport'")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.createOrReplaceTempView("airports")

    airports.foreach(a => println(a))
    val aggregates = spark.sql(
      "select avg(runways) as avg_run from airports"
    )

    aggregates.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation =>
      assertTrue(p.toString().contains("AVG(`runways`)"))
    }

    assertEquals(4.5, aggregates.first().getAs[Double]("avg_run"))
  }

  @Test
  def testScopedQuery(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

  // Spark 4 turns comparisons on collated string columns into CollatedFilters, which the connector cannot compile
  // to SQL++.  They must be handed back to Spark to evaluate, rather than dropped (which returned every row).
  @Test
  def testUnsupportedFilterIsEvaluatedBySpark(): Unit = {
    val ids = spark.read
      .format("couchbase.query")
      .schema("__META_ID STRING, name STRING, country STRING COLLATE UTF8_LCASE")
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()
      .filter(col("country") === "united states")
      .collect()
      .map(_.getAs[String]("__META_ID"))
      .sorted
      .toSeq

    assertEquals(Seq("airport::ord", "airport::sfo"), ids)
  }

  private def airportIdsWhere(condition: org.apache.spark.sql.Column): Seq[String] =
    spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()
      .filter(condition)
      .collect()
      .map(_.getAs[String]("__META_ID"))
      .sorted
      .toSeq

  @Test
  def testFilterOnValueContainingQuote(): Unit = {
    assertEquals(
      Seq("airport::ord"),
      airportIdsWhere(col("name") === "O'Hare International Airport")
    )
    assertEquals(Seq("airport::ord"), airportIdsWhere(col("name").contains("O'Hare")))
  }

  @Test
  def testLikeFilterValueContainingDot(): Unit = {
    assertEquals(Seq(), airportIdsWhere(col("name").startsWith("O'Hare Int.")))
    assertEquals(Seq(), airportIdsWhere(col("name").endsWith(".")))
  }
}
