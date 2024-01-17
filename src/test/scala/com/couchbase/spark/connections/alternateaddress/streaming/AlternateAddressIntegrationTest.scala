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

import com.couchbase.spark.kv.KeyValueOptions
import com.couchbase.spark.util.TestInfraBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{Test, TestInstance}

/** Tests alternate addresses.
  *
  * Spin up a CouchbaseContainer. The cluster config for this exposes "alternateAddresses". Check
  * that we can connect using these when using streaming (DCP).
  */
@TestInstance(Lifecycle.PER_CLASS)
class AlternateAddressIntegrationTest {

  private def test(customiser: (SparkSession.Builder) => Unit) {
    val infra = TestInfraBuilder()
      .createBucketScopeAndCollection("AlternateAddressIntegrationTest")
      .connectToSpark((builder, params) => {
        builder
          .config("spark.couchbase.connectionString", params.connectionString)
          .config("spark.couchbase.username", params.username)
          .config("spark.couchbase.password", params.password)
          .config("spark.couchbase.implicitBucket", params.bucketName)

        customiser.apply(builder)
      })
      .prepareAirportSampleData()

    try {
      val spark = infra.spark

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
    }
    finally {
      infra.stop()
    }
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
