
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

import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.spark.config.CouchbaseConnection
import com.couchbase.spark.connections.MultiClusterConnectionTestUtil.runStandardRDDQuery
import com.couchbase.spark.kv.LookupIn
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

import java.util.UUID

/** Tests multiple cluster connections where they are dynamically setup, against a regular config, RDD operations
 * and a localhost cluster.
 *
 * These tests are for manual testing and hence are disabled.
 */
@Disabled
@TestInstance(Lifecycle.PER_CLASS)
class MultiClusterConnectionLocalhostRDDIntegrationTest {

  var spark: SparkSession           = _

  private val bucketName = UUID.randomUUID().toString

  @BeforeAll
  def setup(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
  }

  @AfterAll
  def teardown(): Unit = {
    CouchbaseConnection().stop()
    spark.stop()
  }

  @Test
  def missingPort(): Unit = {
    val id = s"couchbase://Administrator:password@localhost?spark.couchbase.implicitBucket=${bucketName}"
    runStandardRDDQuery(spark, id)
  }
}
