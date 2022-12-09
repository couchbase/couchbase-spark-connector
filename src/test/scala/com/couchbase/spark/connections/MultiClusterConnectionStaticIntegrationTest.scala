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
package com.couchbase.spark.connections

import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.client.scala.manager.collection.CollectionSpec
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.connections.MultiClusterConnectionStaticIntegrationTest.prepareSampleData
import com.couchbase.spark.kv.LookupIn
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}

import java.util.UUID

/** Tests multiple cluster connections where they are statically setup (config time).
 */
@TestInstance(Lifecycle.PER_CLASS)
class MultiClusterConnectionStaticIntegrationTest {

  var container: CouchbaseContainer = _
  var spark: SparkSession           = _

  private val connectionIdentifier = "test:one"
  private val bucketName = UUID.randomUUID().toString
  private val scopeName = UUID.randomUUID().toString
  private val airportCollectionName = UUID.randomUUID().toString

  @BeforeAll
  def setup(): Unit = {
    container = new CouchbaseContainer("couchbase/server:7.0.3")
      .withBucket(new BucketDefinition(bucketName))
    container.start()

    prepareSampleData(container, bucketName, scopeName, airportCollectionName)

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config(s"spark.couchbase.connectionString:${connectionIdentifier}", container.getConnectionString)
      .config(s"spark.couchbase.username:${connectionIdentifier}", container.getUsername)
      .config(s"spark.couchbase.password:${connectionIdentifier}", container.getPassword)
      .config(s"spark.couchbase.implicitBucket:${connectionIdentifier}", bucketName)
      .getOrCreate()

    spark.conf.getAll.foreach(c => println(s"Initial: ${c._1} = ${c._2}"))
  }

  @AfterAll
  def teardown(): Unit = {
    CouchbaseConnection().stop()
    spark.stop()
    container.stop()
  }

  @Test
  def basic(): Unit = {
    import com.couchbase.spark._

    val result = spark.sparkContext
      .couchbaseLookupIn(Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))), connectionIdentifier = connectionIdentifier)
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }
}

object MultiClusterConnectionStaticIntegrationTest {
  def prepareSampleData(container: CouchbaseContainer, bucketName: String, scopeName: String, collectionName: String): Unit = {
    val initial = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config(s"spark.couchbase.connectionString", container.getConnectionString)
      .config(s"spark.couchbase.username", container.getUsername)
      .config(s"spark.couchbase.password", container.getPassword)
      .config(s"spark.couchbase.implicitBucket", bucketName)
      .getOrCreate()

    val bucket =
      CouchbaseConnection().bucket(CouchbaseConfig(initial.sparkContext.getConf), Some(bucketName))

    bucket.collections.createScope(scopeName)
    bucket.collections.createCollection(CollectionSpec(collectionName, scopeName))

    val airports = initial.read
      .json("src/test/resources/airports.json")
      .withColumn("type", lit("airport"))

    airports.write.format("couchbase.kv").save()

    initial.stop()
  }
}