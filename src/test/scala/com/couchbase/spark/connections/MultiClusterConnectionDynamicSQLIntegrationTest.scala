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
import com.couchbase.spark.connections.MultiClusterConnectionTestUtil.{prepareSampleData, runStandardSQLQuery}
import com.couchbase.spark.util.ClusterVersions.testContainer
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}

import java.util.UUID

/** Tests multiple cluster connections where they are dynamically setup, against a regular config, with SparkSQL
 * operations.
 */
@TestInstance(Lifecycle.PER_CLASS)
class MultiClusterConnectionDynamicSQLIntegrationTest {

  var container: CouchbaseContainer = _
  var spark: SparkSession           = _

  private val bucketName = UUID.randomUUID().toString
  private val scopeName = UUID.randomUUID().toString
  private val airportCollectionName = UUID.randomUUID().toString

  @BeforeAll
  def setup(): Unit = {
    container = testContainer(bucketName)
    container.start()

    prepareSampleData(container, bucketName, scopeName, airportCollectionName)

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.couchbase.connectionString", container.getConnectionString)
      .config("spark.couchbase.username", container.getUsername)
      .config("spark.couchbase.password", container.getPassword)
      .config("spark.couchbase.implicitBucket", bucketName)
      .getOrCreate()
  }

  @AfterAll
  def teardown(): Unit = {
    CouchbaseConnection().stop()
    spark.stop()
    container.stop()
  }


  @Test
  def withAllRequiredSettings(): Unit = {
    val id = s"couchbase://${container.getUsername}:${container.getPassword}@${container.getHost}:${container.getBootstrapCarrierDirectPort}?spark.couchbase.implicitBucket=${bucketName}"
    runStandardSQLQuery(spark, id)
  }

  @Test
  def missingUsernameAndPassword(): Unit = {
    val id = s"couchbase://${container.getHost}:${container.getBootstrapCarrierDirectPort}?spark.couchbase.implicitBucket=${bucketName}"
    runStandardSQLQuery(spark, id)
  }

  @Test
  def minimalSettings(): Unit = {
    val id = s"couchbase://${container.getHost}:${container.getBootstrapCarrierDirectPort}"
    runStandardSQLQuery(spark, id)
  }

  @Test
  def missingImplicitBucket(): Unit = {
    val id = s"couchbase://${container.getUsername}:${container.getPassword}@${container.getHost}:${container.getBootstrapCarrierDirectPort}"
    runStandardSQLQuery(spark, id)
  }
}
