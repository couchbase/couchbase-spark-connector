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
package com.couchbase.spark.connections

import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.spark.kv.LookupIn
import com.couchbase.spark.toSparkContextFunctions
import com.couchbase.spark.util.{Params, SparkOperationalTest}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/** Tests multiple cluster connections where they are statically setup (config time), with RDD
  * operations. The difference to MultiClusterConnectionStaticRDDIntegrationTest is that this has
  * both a default and named connection. Added for SPARKC-178.
  */
class MultiClusterConnectionWithDefaultConnectionIntegrationTest extends SparkOperationalTest {
  override def testName: String = super.testName


  private val connectionIdentifier = "test:one"

  override def sparkBuilderCustomizer(builder: SparkSession.Builder, params: Params): Unit = {
    builder
      .config(s"spark.couchbase.connectionString", params.connectionString)
      .config(s"spark.couchbase.username", params.username)
      .config(s"spark.couchbase.password", params.password)
      .config(s"spark.couchbase.implicitBucket", params.bucketName)
      .config(
        s"spark.couchbase.connectionString:${connectionIdentifier}",
        params.connectionString
      )
      .config(s"spark.couchbase.username:${connectionIdentifier}", params.username)
      .config(s"spark.couchbase.password:${connectionIdentifier}", params.password)
      .config(s"spark.couchbase.implicitBucket:${connectionIdentifier}", params.bucketName)
  }

  @Test
  def basic(): Unit = {
    // Use the default connection
    val result = spark.sparkContext
      .couchbaseLookupIn(Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))))
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }
}
