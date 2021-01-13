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

package com.couchbase.spark.rdd

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.test.SparkIntegrationTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{Test, TestInstance}

import com.couchbase.spark._

@TestInstance(Lifecycle.PER_CLASS)
class GetRDDSpec extends SparkIntegrationTest  {

  @Test
  def fetchesDocuments(): Unit = {
    val collection = connection().collection(couchbaseConfig, None, None, None)

    val user = JsonObject("name" -> "Michael", "age" -> 33, "admin" -> true)
    collection.upsert("user-a", user)

    val rdd = sparkSession().sparkContext.couchbaseGet(Array("user-a", "user-b"))

    val results = rdd.collect()
    assertEquals(1, results.length)
    assertEquals("user-a", results(0).id)
  }

}
