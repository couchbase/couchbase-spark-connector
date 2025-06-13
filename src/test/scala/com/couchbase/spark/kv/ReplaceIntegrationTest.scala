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

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.spark.util.SparkOperationalSimpleTest
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeAll, Test}

class ReplaceIntegrationTest extends SparkOperationalSimpleTest {
  val ErrorsCollection = "replace_errors"

  @BeforeAll
  def setupTest(): Unit = {
    testResourceCreator.createCollection(testResources.bucketName, testResources.scopeName, Some(ErrorsCollection))
  }

  private def createTestData() = {
    val sp = spark
    import sp.implicits._
    Seq(
      ("doc1", "value1"),
      ("doc2", "value2"),
      ("doc3", "value3")
    ).toDF("__META_ID", "content")
  }

  private def createUpdatedData() = {
    val sp = spark
    import sp.implicits._
    Seq(
      ("doc1", "updated1"),
      ("doc2", "updated2"),
      ("doc3", "updated3")
    ).toDF("__META_ID", "content")
  }

  private def createDataWithMissingDoc() = {
    val sp = spark
    import sp.implicits._
    Seq(
      ("doc1", "replaced1"),
      ("doc2", "replaced2"),
      ("nonexistent", "new_value")
    ).toDF("__META_ID", "content")
  }

  private def writeInitial(testData: org.apache.spark.sql.DataFrame): Unit = {
    testData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def performReplace(testData: org.apache.spark.sql.DataFrame): Unit = {
    testData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def performReplaceWithErrorHandler(testData: org.apache.spark.sql.DataFrame): Unit = {
    testData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
      .option(KeyValueOptions.ErrorHandler, "com.couchbase.spark.kv.TestLoggingErrorHandler")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def performReplaceWithErrorDocuments(testData: org.apache.spark.sql.DataFrame): Unit = {
    testData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
      .option(KeyValueOptions.ErrorBucket, testResources.bucketName)
      .option(KeyValueOptions.ErrorScope, testResources.scopeName)
      .option(KeyValueOptions.ErrorCollection, ErrorsCollection)
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def verifyContent(docId: String, expectedContent: String): Unit = {
    val result = spark.read
      .format("couchbase.query")
      .option("bucket", testResources.bucketName)
      .option("scope", testResources.scopeName)
      .option("collection", testResources.collectionName)
      .option("filter", s"META().id = '$docId'")
      .load()
      .collect()

    assertEquals(1, result.length)
    assertEquals(expectedContent, result.head.getAs[String]("content"))
  }

  @Test
  def testBasicReplace(): Unit = {
    val testData = createTestData()
    val updatedData = createUpdatedData()

    writeInitial(testData)
    performReplace(updatedData)

    verifyContent("doc1", "updated1")
    verifyContent("doc2", "updated2")
    verifyContent("doc3", "updated3")
  }

  @Test
  def testReplaceWithNonExistentDocumentFails(): Unit = {
    val testData = createTestData()
    val mixedData = createDataWithMissingDoc()

    writeInitial(testData)

    assertThrows(classOf[Exception], () => {
      performReplace(mixedData)
    })
  }

  @Test
  def testReplaceWithErrorHandler(): Unit = {
    TestLoggingErrorHandler.clear()

    val testData = createTestData()
    val mixedData = createDataWithMissingDoc()

    writeInitial(testData)
    performReplaceWithErrorHandler(mixedData)

    verifyContent("doc1", "replaced1")
    verifyContent("doc2", "replaced2")

    val errorInfos = TestLoggingErrorHandler.getErrorInfos
    assertTrue(errorInfos.nonEmpty)

    val nonExistentError = errorInfos.find(_.documentId == "nonexistent")
    assertTrue(nonExistentError.isDefined)

    val errorInfo = nonExistentError.get
    assertTrue(errorInfo.throwable.get.isInstanceOf[DocumentNotFoundException])

    TestLoggingErrorHandler.clear()
  }

  @Test
  def testReplaceWithErrorDocuments(): Unit = {
    val testData = createTestData()
    val mixedData = createDataWithMissingDoc()

    writeInitial(testData)
    performReplaceWithErrorDocuments(mixedData)

    verifyContent("doc1", "replaced1")
    verifyContent("doc2", "replaced2")

    val errorDocs = spark.read
      .format("couchbase.query")
      .option("bucket", testResources.bucketName)
      .option("scope", testResources.scopeName)
      .option("collection", ErrorsCollection)
      .load()
      .collect()

    assertTrue(errorDocs.nonEmpty)

    val errorDoc = errorDocs.find(_.getAs[String]("documentId") == "nonexistent")
    assertTrue(errorDoc.isDefined)

    val doc = errorDoc.get
    assertEquals("nonexistent", doc.getAs[String]("documentId"))
  }

  @Test
  def testInvalidWriteModeThrowsException(): Unit = {
    val testData = createTestData()

    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      testData.write
        .format("couchbase.kv")
        .option("bucket", testResources.bucketName)
        .option("scope", testResources.scopeName)
        .option("collection", testResources.collectionName)
        .option(KeyValueOptions.WriteMode, "invalid-mode")
        .mode(SaveMode.Overwrite)
        .save()
    })

    assertTrue(exception.getMessage.contains("Unknown KeyValueOptions.WriteMode option: invalid-mode"))
  }

  override def testName: String = "ReplaceIntegrationTest"
}