/*
 * Copyright (c) 2026 Couchbase, Inc.
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
import com.couchbase.spark.Keyspace
import com.couchbase.spark.util.SparkOperationalSimpleTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeAll, BeforeEach, Test}

/** SaveMode.Append is not supported by couchbase.kv, and must be rejected before anything is
  * written.
  */
class SaveModeAppendIntegrationTest extends SparkOperationalSimpleTest {
  private val TestCollection = "savemode_append_test"

  override def testName: String = "savemodeappend"

  private def keyspace = Keyspace(
    bucket = Some(testResources.bucketName),
    scope = Some(testResources.scopeName),
    collection = Some(TestCollection)
  )

  @BeforeAll
  def setUpCollection(): Unit = {
    testResourceCreator.createCollection(
      testResources.bucketName,
      testResources.scopeName,
      Some(TestCollection)
    )
  }

  @BeforeEach
  def writeInitialDocs(): Unit = {
    val sp = spark
    import sp.implicits._
    Seq(("doc1", "original1", Seq("a")), ("doc2", "original2", Seq("a")))
      .toDF("__META_ID", "content", "tags")
      .write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, TestCollection)
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def writeWithAppend(df: DataFrame, writeMode: Option[String]): IllegalArgumentException = {
    var writer = df.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, TestCollection)
      .mode(SaveMode.Append)
    writeMode.foreach(m => writer = writer.option(KeyValueOptions.WriteMode, m))
    assertThrows(classOf[IllegalArgumentException], () => writer.save())
  }

  private def assertUnchanged(): Unit = {
    import com.couchbase.spark._
    val docs = spark.sparkContext
      .couchbaseGet(Seq(Get("doc1"), Get("doc2")), keyspace)
      .collect()
      .map(r => r.id -> r.contentAs[JsonObject].get)
      .toMap
    assertEquals(2, docs.size)
    assertEquals("original1", docs("doc1").str("content"))
    assertEquals("original2", docs("doc2").str("content"))
    assertEquals(1, docs("doc1").arr("tags").size)
    assertEquals(1, docs("doc2").arr("tags").size)
  }

  @Test
  def appendWithoutWriteModeIsRejected(): Unit = {
    val sp = spark
    import sp.implicits._
    val df = Seq(("doc1", "updated1"), ("doc2", "updated2")).toDF("__META_ID", "content")

    val e = writeWithAppend(df, None)
    assertTrue(e.getMessage.contains("SaveMode.Append is not supported"), e.getMessage)
    assertUnchanged()
  }

  @Test
  def appendWithReplaceWriteModeIsRejectedBeforeWriting(): Unit = {
    val sp = spark
    import sp.implicits._
    val df = Seq(("doc1", "updated1"), ("doc2", "updated2")).toDF("__META_ID", "content")

    val e = writeWithAppend(df, Some(KeyValueOptions.WriteModeReplace))
    assertTrue(e.getMessage.contains("SaveMode.Append is not supported"), e.getMessage)
    assertUnchanged()
  }

  @Test
  def appendWithSubdocWriteModeIsRejectedBeforeWriting(): Unit = {
    val sp = spark
    import sp.implicits._
    // arrayAppend is not idempotent, so applying it and then failing the job is especially harmful.
    val df = Seq(("doc1", "b"), ("doc2", "b")).toDF("__META_ID", "arrayAppend:tags")

    val e = writeWithAppend(df, Some(KeyValueOptions.WriteModeSubdocUpsert))
    assertTrue(e.getMessage.contains("SaveMode.Append is not supported"), e.getMessage)
    assertUnchanged()
  }
}
