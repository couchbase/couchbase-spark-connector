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

import com.couchbase.spark.connections.MultiClusterConnectionTestUtil.runStandardRDDQuery
import com.couchbase.spark.util.{Params, SparkOperationalTest}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Test

/** Tests multiple cluster connections where they are statically setup (config time), with RDD operations.
 */
class MultiClusterConnectionStaticRDDIntegrationTest extends SparkOperationalTest {
  override def testName: String = super.testName


  private val connectionIdentifier = "test:one"

  override def sparkBuilderCustomizer(builder: SparkSession.Builder, params: Params): Unit = {
    builder
      .config(s"spark.couchbase.connectionString:${connectionIdentifier}", params.connectionString) // filtered
      .config(s"spark.couchbase.username:${connectionIdentifier}", infra.params.username) // filtered
      .config(s"spark.couchbase.password:${connectionIdentifier}", infra.params.password) // filtered
      .config(s"spark.couchbase.connectionString", params.connectionString) // filtered
      .config(s"spark.couchbase.username", infra.params.username) // filtered
      .config(s"spark.couchbase.password", infra.params.password) // filtered
      .config(s"spark.couchbase.implicitBucket", params.bucketName) // filtered
      .config(s"spark.couchbase.implicitBucket:${connectionIdentifier}", params.bucketName) // filtered
      .config(s"spark.couchbase.connectionString:xxxx${connectionIdentifier}", params.connectionString) // filtered
      .config(s"spark.couchbase.username:xxxx${connectionIdentifier}", infra.params.username) // filtered
      .config(s"spark.couchbase.password:xxxx${connectionIdentifier}", infra.params.password) // filtered
      .config(s"spark.couchbase.implicitBucket:xxxx${connectionIdentifier}", params.bucketName) // filtered
      .config(s"spark.couchbase.maxNumRequestsInRetry:xxxx${connectionIdentifier}", 77) // filtered
      .config(s"spark.couchbase.maxNumRequestsInRetry:${connectionIdentifier}", 88) // not filtered, connectionId-ed, but will override the 4
      .config(s"spark.couchbase.maxNumRequestsInRetry", 4) // not filtered, default

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
