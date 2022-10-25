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

import com.couchbase.client.scala.manager.collection.CollectionSpec
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.kv.KeyValueOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}

import java.util.UUID

@TestInstance(Lifecycle.PER_CLASS)
class QueryDataFrameCollectionsIntegrationTest {

  var container: CouchbaseContainer = _
  var spark: SparkSession           = _

  private val bucketName            = UUID.randomUUID().toString
  private val scopeName             = UUID.randomUUID().toString
  private val airportCollectionName = UUID.randomUUID().toString

  @BeforeAll
  def setup(): Unit = {
    container = new CouchbaseContainer("couchbase/server:7.0.3")
      .withBucket(new BucketDefinition(bucketName))
    container.start()

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.couchbase.connectionString", container.getConnectionString)
      .config("spark.couchbase.username", container.getUsername)
      .config("spark.couchbase.password", container.getPassword)
      .config("spark.couchbase.implicitBucket", bucketName)
      .getOrCreate()

    val bucket =
      CouchbaseConnection().bucket(CouchbaseConfig(spark.sparkContext.getConf), Some(bucketName))

    bucket.collections.createScope(scopeName)
    bucket.collections.createCollection(CollectionSpec(airportCollectionName, scopeName))

    bucket.scope(scopeName).query(s"create primary index on `$airportCollectionName`")

    prepareSampleData()
  }

  @AfterAll
  def teardown(): Unit = {
    CouchbaseConnection().stop()
    container.stop()
    spark.stop()
  }

  private def prepareSampleData(): Unit = {
    val airports = spark.read
      .json("src/test/resources/airports.json")

    airports.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Scope, scopeName)
      .option(KeyValueOptions.Collection, airportCollectionName)
      .save()
  }

  @Test
  def testReadDocumentsFromCollection(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, scopeName)
      .option(QueryOptions.Collection, airportCollectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

  @Test
  def testChangeIdFieldName(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.IdFieldName, "myIdFieldName")
      .option(QueryOptions.Scope, scopeName)
      .option(QueryOptions.Collection, airportCollectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.foreach(row => {
      assertThrows(classOf[IllegalArgumentException], () => row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("myIdFieldName"))
    })
  }

  @Test
  def testPushDownAggregationWithoutGroupBy(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, scopeName)
      .option(QueryOptions.Collection, airportCollectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.createOrReplaceTempView("airports")

    val aggregates = spark.sql("select max(elevation) as el, min(runways) as run from airports")

    aggregates.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation =>
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
      .option(QueryOptions.Scope, scopeName)
      .option(QueryOptions.Collection, airportCollectionName)
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

}
