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

package com.couchbase.spark.sql

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.test.{Capabilities, ClusterType, IgnoreWhen, SparkIntegrationTest}
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Test, TestInstance}

@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED), missesCapabilities = Array(Capabilities.QUERY))
@TestInstance(Lifecycle.PER_CLASS)
class QueryRelationSpec extends SparkIntegrationTest {

  @BeforeAll
  def setup(): Unit = {
    cluster.queryIndexes.createPrimaryIndex(bucketName()).get
  }

  @Test
  def shouldInferSchema(): Unit = {
    val collection = connection().collection(couchbaseConfig, None, None, None)

    val user = JsonObject("name" -> "Michael", "age" -> 33, "admin" -> true)
    collection.upsert("user-a", user)

    val options = Map[String, String]()

    val users = sparkSession.sqlContext.read.couchbaseQuery(options = options)

    val expectedSchema = StructType(Array(
      StructField("META_ID", StringType, nullable = true),
      StructField("admin", BooleanType, nullable = true),
      StructField("age", LongType, nullable = true),
      StructField("name", StringType, nullable = true),
    ))

    assertEquals(expectedSchema, users.schema)

    val found = users.select("admin").where("name = 'Michael'").collect()
    assertTrue(found(0).getAs[Boolean]("admin"))
    assertEquals(1, found.length)
  }

}