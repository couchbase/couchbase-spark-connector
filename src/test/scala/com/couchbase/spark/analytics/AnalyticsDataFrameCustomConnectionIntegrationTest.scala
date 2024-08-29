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
package com.couchbase.spark.analytics

import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.util.{Params, SparkOperationalTest, TestInfraBuilder, TestInfraConnectedToSpark}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

class AnalyticsDataFrameCustomConnectionIntegrationTest extends SparkOperationalTest {

  override def testName: String = super.testName

  override def sparkBuilderCustomizer(builder: SparkSession.Builder, params: Params): Unit = {
    builder
      .config("spark.couchbase.connectionString:custom", params.connectionString)
      .config("spark.couchbase.username:custom", params.username)
      .config("spark.couchbase.password:custom", params.password)
      .config("spark.couchbase.implicitBucket:custom", params.bucketName)
  }

  @BeforeAll
  def setupTest(): Unit = {
    val identifier = Some("custom")
    val config     = CouchbaseConfig(spark.sparkContext.getConf, identifier)
    val cluster    = CouchbaseConnection(identifier).cluster(config)

    cluster.analyticsIndexes.createDataset("airports", infra.params.bucketName)
    cluster.analyticsQuery("connect link Local").get
  }

  @Test
  def testReadDocumentsFromDataset(): Unit = {
    val airports = spark.read
      .format("couchbase.analytics")
      .option(AnalyticsOptions.Dataset, "airports")
      .option(AnalyticsOptions.ScanConsistency, AnalyticsOptions.RequestPlusScanConsistency)
      .option(AnalyticsOptions.ConnectionIdentifier, "custom")
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

}
