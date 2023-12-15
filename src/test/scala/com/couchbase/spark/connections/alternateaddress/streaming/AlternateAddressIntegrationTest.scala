/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.spark.connections.alternateaddress.streaming

import com.couchbase.spark.config.CouchbaseConnection
import com.couchbase.spark.kv.KeyValueOptions
import com.couchbase.spark.util.ClusterVersions.testContainer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}

import java.util.UUID

/** Tests alternate addresses.
  *
  * Spin up a CouchbaseContainer. The cluster config for this exposes "alternateAddresses". Check
  * that we can connect using these when using streaming (DCP).
  */
@TestInstance(Lifecycle.PER_CLASS)
class AlternateAddressIntegrationTest {

  var container: CouchbaseContainer = _
  var spark: SparkSession           = _

  private val bucketName = UUID.randomUUID().toString

  @BeforeAll
  def setup(): Unit = {
    container = testContainer(bucketName)
    container.start()
  }

  @AfterAll
  def teardown(): Unit = {
    container.stop()
  }

  private def test(customiser: (SparkSession.Builder) => Unit) {
    val builder = SparkSession
      .builder()
      .master("local[*]")
      .appName("Structured Streaming Example")
      .config("spark.couchbase.connectionString", container.getConnectionString)
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.implicitBucket", bucketName)

    customiser.apply(builder)

    val spark = builder.getOrCreate()

    val sourceDf = spark.readStream
      .format("couchbase.kv")
      .option(KeyValueOptions.StreamFrom, KeyValueOptions.StreamFromBeginning)
      .load

    val aggDf = sourceDf.groupBy("collection").count()

    val query = aggDf.writeStream
      .format("console")
      .outputMode(OutputMode.Complete)
      .trigger(Trigger.Once())
      .queryName("kv2console")
      .start

    query.awaitTermination()

    CouchbaseConnection().stop()
    spark.stop()
  }

  @Test
  def withDefaultConfig(): Unit = {
    test(_ => {})
  }

  @Test
  def withNetworkResolutionExternal(): Unit = {
    test(builder => builder.config("spark.couchbase.io.networkResolution", "external"))
  }
}
