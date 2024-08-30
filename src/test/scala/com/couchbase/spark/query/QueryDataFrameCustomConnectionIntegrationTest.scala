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
package com.couchbase.spark.query

import com.couchbase.spark.kv.KeyValueOptions
import com.couchbase.spark.util.{CreatedResources, Params, RequiresOperationalCluster, SparkOperationalTest, SparkSessionHelper, TestNameUtil, TestOptionsPropertyLoader, TestResourceCreator}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(Array(classOf[RequiresOperationalCluster]))
@TestInstance(Lifecycle.PER_CLASS)
class QueryDataFrameCustomConnectionIntegrationTest {
  protected var spark: SparkSession = _
  protected val params = new TestOptionsPropertyLoader()
  protected val testResourceCreator = new TestResourceCreator(params)
  protected var testResources: CreatedResources = _

  @BeforeAll
  def setup(): Unit = {
    val builder = SparkSessionHelper.sparkSessionBuilder()
    testResources = testResourceCreator.createBucketScopeAndCollection(Some(TestNameUtil.testName))
    spark = builder
      .config("spark.couchbase.connectionString:custom", params.connectionString)
      .config("spark.couchbase.username:custom", params.username)
      .config("spark.couchbase.password:custom", params.password)
      .config("spark.couchbase.implicitBucket:custom", testResources.bucketName)
      .getOrCreate()

    // Cannot use prepareAirportSampleData as we have a custom connectionIdentifier
    val airports = spark.read
      .json("src/test/resources/airports.json")
    airports
      .withColumn("type", lit("airport"))
      .write
      .format("couchbase.kv")
      .mode(SaveMode.Overwrite)
      .option(KeyValueOptions.ConnectionIdentifier, "custom")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .save()
  }

  @AfterAll
  def teardown(): Unit = {
    spark.stop()
    testResourceCreator.deleteBucket(testResources.bucketName)
    testResourceCreator.stop()
  }

  @Test
  def testReadDocumentsWithFilter(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airport'")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .option(QueryOptions.ConnectionIdentifier, "custom")
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertEquals("airport", row.getAs[String]("type"))
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

  @Test
  def testUsesUnspecifiedDefaultConnection(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        spark.read
          .format("couchbase.query")
          .load()
    )

    assertEquals(
      ex.getMessage,
      "Required config property spark.couchbase.connectionString is not present"
    )
  }

  @Test
  def testUsesDifferentCustomConnection(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        spark.read
          .format("couchbase.query")
          .option(QueryOptions.ConnectionIdentifier, "customOther")
          .load()
    )

    assertEquals(
      ex.getMessage,
      "Required config property spark.couchbase.connectionString:customOther is not present"
    )
  }

}
