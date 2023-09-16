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

import com.couchbase.spark.config.CouchbaseConnection
import com.couchbase.spark.connections.MultiClusterConnectionTestUtil.{prepareSampleData, runStandardRDDQuery}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}

import java.util.UUID

/** Tests multiple cluster connections where they are statically setup (config time), with RDD operations.
 */
@TestInstance(Lifecycle.PER_CLASS)
class MultiClusterConnectionStaticRDDIntegrationTest {

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
      .config(s"spark.couchbase.connectionString:${connectionIdentifier}", container.getConnectionString) // filtered
      .config(s"spark.couchbase.username:${connectionIdentifier}", container.getUsername) // filtered
      .config(s"spark.couchbase.password:${connectionIdentifier}", container.getPassword) // filtered
      .config(s"spark.couchbase.connectionString", container.getConnectionString) // filtered
      .config(s"spark.couchbase.username", container.getUsername) // filtered
      .config(s"spark.couchbase.password", container.getPassword) // filtered
      .config(s"spark.couchbase.implicitBucket", bucketName) // filtered
      .config(s"spark.couchbase.implicitBucket:${connectionIdentifier}", bucketName) // filtered
      .config(s"spark.couchbase.connectionString:xxxx${connectionIdentifier}", container.getConnectionString) // filtered
      .config(s"spark.couchbase.username:xxxx${connectionIdentifier}", container.getUsername) // filtered
      .config(s"spark.couchbase.password:xxxx${connectionIdentifier}", container.getPassword) // filtered
      .config(s"spark.couchbase.implicitBucket:xxxx${connectionIdentifier}", bucketName) // filtered
      .config(s"spark.couchbase.maxNumRequestsInRetry:xxxx${connectionIdentifier}", 77) // filtered
      .config(s"spark.couchbase.maxNumRequestsInRetry:${connectionIdentifier}", 88) // not filtered, connectionId-ed, but will override the 4
      .config(s"spark.couchbase.maxNumRequestsInRetry", 4) // not filtered, default
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
  def basicWithConnectionIdentifier(): Unit = {
    runStandardRDDQuery(spark, connectionIdentifier)
  }

  @Test
  def basic(): Unit = {
    runStandardRDDQuery(spark)
  }
}
