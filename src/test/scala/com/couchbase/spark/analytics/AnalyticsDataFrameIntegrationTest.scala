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

import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer, CouchbaseService}

import java.util.UUID

@TestInstance(Lifecycle.PER_CLASS)
class AnalyticsDataFrameIntegrationTest {

  var container: CouchbaseContainer = _
  var spark: SparkSession = _

  @BeforeAll
  def setup(): Unit = {
    val bucketName: String = UUID.randomUUID().toString

    container = new CouchbaseContainer("couchbase/server:6.6.2")
      .withEnabledServices(CouchbaseService.KV, CouchbaseService.ANALYTICS)
      .withBucket(new BucketDefinition(bucketName).withPrimaryIndex(false))
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

    val cluster = CouchbaseConnection().cluster(CouchbaseConfig(spark.sparkContext.getConf))
    cluster.analyticsIndexes.createDataset("airports", bucketName)
    cluster.analyticsQuery("connect link Local").get

    prepareSampleData()
  }

  @AfterAll
  def teardown(): Unit = {
    container.stop()
  }

  private def prepareSampleData(): Unit = {
    val airports = spark
      .read
      .json("src/test/resources/airports.json")
      .withColumn("type", lit("airport"))

    airports.write.format("couchbase.kv").save()
  }

  @Test
  def testReadDocumentsFromDataset(): Unit = {
    val airports = spark.read
      .format("couchbase.analytics")
      .option(AnalyticsOptions.Dataset, "airports")
      .option(AnalyticsOptions.ScanConsistency, AnalyticsOptions.RequestPlusScanConsistency)
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
      .format("couchbase.analytics")
      .option(AnalyticsOptions.IdFieldName, "myIdFieldName")
      .option(AnalyticsOptions.Dataset, "airports")
      .option(AnalyticsOptions.ScanConsistency, AnalyticsOptions.RequestPlusScanConsistency)
      .load()

    airports.foreach(row => {
      assertThrows(classOf[IllegalArgumentException], () => row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("myIdFieldName"))
    })
  }

}
