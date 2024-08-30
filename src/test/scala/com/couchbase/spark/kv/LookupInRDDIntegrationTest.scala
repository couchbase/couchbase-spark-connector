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
package com.couchbase.spark.kv

import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.spark.util.{SparkOperationalSimpleTest, SparkOperationalTest, TestNameUtil}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class LookupInRDDIntegrationTest extends SparkOperationalSimpleTest {
  override def testName: String = TestNameUtil.testName

  @Test
  def testLookupFromDefaultCollection(): Unit = {
    import com.couchbase.spark._

    val result = spark.sparkContext
      .couchbaseLookupIn(Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))))
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }

  @Test
  def testLookupFromCustomCollection(): Unit = {
    import com.couchbase.spark._

    val result = spark.sparkContext
      .couchbaseLookupIn(
        Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))),
        Keyspace(
          scope = Some(testResources.scopeName),
          collection = Some(testResources.collectionName)
        )
      )
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }

}
