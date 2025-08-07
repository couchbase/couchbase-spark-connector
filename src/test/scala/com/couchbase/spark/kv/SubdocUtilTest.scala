/*
 * Copyright (c) 2025 Couchbase, Inc.
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

import com.couchbase.client.scala.json.JsonObject
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SubdocUtilTest {
  private var spark: SparkSession = _

  @BeforeAll
  def setupSpark(): Unit = {
    spark = SparkSession.builder().master("local[1]").appName("SubdocUtilTest").getOrCreate()
  }

  @AfterAll
  def stopSpark(): Unit = {
    if (spark != null) spark.stop()
  }

  private val idField = "__META_ID"
  private val casField = "__META_CAS"

  @Test
  def `fails when more than 16 specs`(): Unit = {
    val fields = (1 to 17).map(i => StructField(s"upsert:path$i", StringType, nullable = true)) :+
      StructField(idField, StringType, nullable = false)
    val schema = StructType(fields)

    val ex = assertThrows(classOf[IllegalArgumentException], () =>
      SubdocUtil.validate(schema, idField, Some(casField))
    )
    assertTrue(ex.getMessage.contains("maximum 16"))
  }

  @Test
  def `fails on unsupported operation`(): Unit = {
    val schema = StructType(Seq(
      StructField("foo:path", StringType), // unsupported op "foo"
      StructField(idField, StringType, nullable = false)
    ))

    val ex = assertThrows(classOf[IllegalArgumentException], () =>
      SubdocUtil.validate(schema, idField, None)
    )
    assertTrue(ex.getMessage.contains("Unsupported sub-document operation"))
  }

  @Test
  def `passes for valid schema`(): Unit = {
    val schema = StructType(Seq(
      StructField("upsert:name", StringType),
      StructField("increment:count", IntegerType),
      StructField(idField, StringType, nullable = false)
    ))

    // Should not throw
    SubdocUtil.validate(schema, idField, None)
  }

  @Test
  def `handles Integer and Float types for replace operation`(): Unit = {
    val jsonObj = JsonObject.create
      .put("replace:score", Integer.valueOf(20))
      .put("replace:average", java.lang.Float.valueOf(85.5f))

    val (specs, _) = SubdocUtil.buildSubdocSpecs(jsonObj)
    assertEquals(2, specs.length)
    
    assertTrue(specs.nonEmpty)
  }

  @Test
  def `handles Integer and Float types for insert operation`(): Unit = {
    val jsonObj = JsonObject.create
      .put("insert:newScore", Integer.valueOf(100))
      .put("insert:newAverage", java.lang.Float.valueOf(92.3f))

    val (specs, _) = SubdocUtil.buildSubdocSpecs(jsonObj)
    assertEquals(2, specs.length)
    
    assertTrue(specs.nonEmpty)
  }

  @Test
  def `handles Integer and Float types for upsert operation`(): Unit = {
    val jsonObj = JsonObject.create
      .put("upsert:score", Integer.valueOf(75))
      .put("upsert:percentage", java.lang.Float.valueOf(88.2f))

    val (specs, _) = SubdocUtil.buildSubdocSpecs(jsonObj)
    assertEquals(2, specs.length)
    
    assertTrue(specs.nonEmpty)
  }

  @Test
  def `handles Integer and Float types for arrayAddUnique operation`(): Unit = {
    val jsonObj = JsonObject.create
      .put("arrayaddunique:scores", Integer.valueOf(95))
      .put("arrayaddunique:percentages", java.lang.Float.valueOf(78.9f))

    val (specs, _) = SubdocUtil.buildSubdocSpecs(jsonObj)
    assertEquals(2, specs.length)
    
    assertTrue(specs.nonEmpty)
  }

  @Test
  def `handles Integer and Float types for increment operation`(): Unit = {
    val jsonObj = JsonObject.create
      .put("increment:counter", Integer.valueOf(5))
      .put("increment:floatCounter", java.lang.Float.valueOf(2.5f))

    val (specs, _) = SubdocUtil.buildSubdocSpecs(jsonObj)
    assertEquals(2, specs.length)
    
    assertTrue(specs.nonEmpty)
  }

  @Test
  def `handles Integer and Float types for decrement operation`(): Unit = {
    val jsonObj = JsonObject.create
      .put("decrement:counter", Integer.valueOf(3))
      .put("decrement:floatCounter", java.lang.Float.valueOf(1.2f))

    val (specs, _) = SubdocUtil.buildSubdocSpecs(jsonObj)
    assertEquals(2, specs.length)
    
    assertTrue(specs.nonEmpty)
  }

  @Test
  def `handles remove operation with null value`(): Unit = {
    val jsonObj = JsonObject.create
      .put("remove:toRemove", null.asInstanceOf[String])
      .put("decrement:counter", Integer.valueOf(100))

    val (specs, debugInfo) = SubdocUtil.buildSubdocSpecs(jsonObj)
    assertEquals(2, specs.length)
    
    assertTrue(debugInfo.contains("remove@toRemove"))
    assertTrue(debugInfo.contains("decrement@counter"))
  }

}
