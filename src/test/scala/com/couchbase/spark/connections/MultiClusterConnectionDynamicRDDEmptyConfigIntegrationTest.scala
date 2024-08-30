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
import com.couchbase.spark.util.{CreatedResources, RequiresOperationalCluster, SparkOperationalTest, SparkSessionHelper, TestResourceCreator, TestInfraBuilder, TestOptionsPropertyLoader}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

/** Tests multiple cluster connections where they are dynamically setup, against an empty config and
  * RDD operations.
  */
@ExtendWith(Array(classOf[RequiresOperationalCluster]))
@TestInstance(Lifecycle.PER_CLASS)
class MultiClusterConnectionDynamicRDDEmptyConfigIntegrationTest {
  private var spark: SparkSession = _
  private val params = new TestOptionsPropertyLoader()
  private val testResources = new TestResourceCreator(params)
  private var resources: CreatedResources = _

  @BeforeAll
  def setup(): Unit = {
    val builder = SparkSessionHelper.sparkSessionBuilder()
    spark = SparkSessionHelper.provideCouchbaseCreds(builder, params).getOrCreate()
    resources = testResources.createBucketScopeAndCollection()
    testResources.prepareAirportSampleData(spark, resources)
  }

  @AfterAll
  def teardown(): Unit = {
    spark.stop()
    testResources.deleteBucket(resources.bucketName)
    testResources.stop()
  }

  @Test
  def withAllRequiredSettings(): Unit = {
    val id =
      s"couchbase://${params.username}:${params.password}@${params.connectionString}?spark.couchbase.implicitBucket=${resources.bucketName}"
    runStandardRDDQuery(spark, id)
  }

  @Test
  def missingImplicitBucket(): Unit = {
    val id = s"couchbase://${}:${params.password}@${params.connectionString}"
    try {
      runStandardRDDQuery(spark, id)
    } catch {
      case _: IllegalArgumentException =>
    }
  }
}
