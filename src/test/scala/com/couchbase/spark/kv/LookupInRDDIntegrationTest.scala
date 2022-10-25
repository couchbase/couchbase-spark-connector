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
package com.couchbase.spark.kv

import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.client.scala.manager.collection.CollectionSpec
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}

import java.util.UUID

@TestInstance(Lifecycle.PER_CLASS)
class LookupInRDDIntegrationTest {

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

    airports
      .withColumn("type", lit("airport"))
      .write
      .format("couchbase.kv")
      .save()

    airports.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Scope, scopeName)
      .option(KeyValueOptions.Collection, airportCollectionName)
      .save()
  }

  @Test
  def testLookupFromDefaultCollection(): Unit = {
    import com.couchbase.spark._

    val result = spark.sparkContext
      .couchbaseLookupIn(Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))))
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }

  @Test
  def testLookupFromCustomCollection(): Unit = {
    import com.couchbase.spark._

    val result = spark.sparkContext
      .couchbaseLookupIn(
        Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))),
        Keyspace(scope = Some(scopeName), collection = Some(airportCollectionName))
      )
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }

}
