/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}

import java.util.UUID

@TestInstance(Lifecycle.PER_CLASS)
class QueryDataFrameIntegrationTest {

  var container: CouchbaseContainer = null

  var spark: SparkSession = null

  @BeforeAll
  def setup(): Unit = {
    val bucketName: String = UUID.randomUUID().toString

    container = new CouchbaseContainer("couchbase/server:6.6.2")
      .withBucket(new BucketDefinition(bucketName).withPrimaryIndex(true))
    container.start()

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL Couchbase Query Integration Test")
      .config("spark.couchbase.connectionString", container.getConnectionString)
      .config("spark.couchbase.username", container.getUsername)
      .config("spark.couchbase.password", container.getPassword)
      .config("spark.couchbase.implicitBucket", bucketName)
      .getOrCreate()
  }

  @AfterAll
  def teardown(): Unit = {
    container.stop()
  }

  @Test
  def test(): Unit = {
    val airlines = spark.read.format("com.couchbase.spark.query.QueryTableProvider")
      .option(QueryOptions.Filter, "type = 'airline'")
      .load()

    airlines.select("country", "iata").where("country = 'United States'").show()
  }


}
