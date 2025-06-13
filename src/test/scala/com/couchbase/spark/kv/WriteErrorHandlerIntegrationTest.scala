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

import com.couchbase.client.core.error.DocumentExistsException
import com.couchbase.spark.util.SparkOperationalSimpleTest
import org.apache.spark.sql.{Row, SaveMode}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeAll, Test}

class TestLoggingErrorHandler extends KeyValueWriteErrorHandler {
  override def onError(errorInfo: KeyValueWriteErrorInfo): Unit = {
    val logMessage   = s"ERROR: Failure on document ${errorInfo.documentId}"
    TestLoggingErrorHandler.addErrorInfo(errorInfo)
    println(logMessage)
  }
}

object TestLoggingErrorHandler {
  import java.util.concurrent.ConcurrentLinkedQueue
  import scala.collection.JavaConverters._

  private val errorInfos = new ConcurrentLinkedQueue[KeyValueWriteErrorInfo]()

  def addErrorInfo(errorInfo: KeyValueWriteErrorInfo): Unit = {
    errorInfos.offer(errorInfo)
  }

  def getErrorInfos: List[KeyValueWriteErrorInfo] = errorInfos.asScala.toList
  def getErrorCount: Int                       = errorInfos.size()
  def clear(): Unit                            = errorInfos.clear()
}

class NonSerializableErrorHandler extends KeyValueWriteErrorHandler {
  private val nonSerializableField = new java.io.FileInputStream("/dev/null")

  override def onError(errorInfo: KeyValueWriteErrorInfo): Unit = {
    println("This handler has non-serializable fields")
  }
}

class WriteErrorHandlerIntegrationTest extends SparkOperationalSimpleTest {
  val ErrorsCollection = "errors"

  @BeforeAll
  def setupTest(): Unit = {
    testResourceCreator.createCollection(
      testResources.bucketName,
      testResources.scopeName,
      Some(ErrorsCollection)
    )
  }

  private def createTestDataWithDuplicates() = {
    val sp = spark
    import sp.implicits._

    Seq(
      ("doc2", "value2"),
      ("doc1", "value1"),
      ("doc1", "value1_duplicate")
    ).toDF("__META_ID", "content")
  }

  private def createSimpleTestData() = {
    val sp = spark
    import sp.implicits._

    Seq(
      ("doc1", "value1"),
      ("doc2", "value2")
    ).toDF("__META_ID", "content")
  }

  private def writeInitialData(testData: org.apache.spark.sql.DataFrame): Unit = {
    testData
      .limit(2)
      .write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def writeWithErrorHandler(
      testData: org.apache.spark.sql.DataFrame,
      errorHandler: String,
      mode: SaveMode = SaveMode.ErrorIfExists
  ): Unit = {
    testData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.ErrorHandler, errorHandler)
      .mode(mode)
      .save()
  }

  private def writeWithErrorDocuments(
      testData: org.apache.spark.sql.DataFrame,
      mode: SaveMode = SaveMode.ErrorIfExists
  ): Unit = {
    testData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.ErrorBucket, testResources.bucketName)
      .option(KeyValueOptions.ErrorScope, testResources.scopeName)
      .option(KeyValueOptions.ErrorCollection, ErrorsCollection)
      .mode(mode)
      .save()
  }

  @Test
  def testTestLoggingErrorHandler(): Unit = {
    TestLoggingErrorHandler.clear()

    val testData = createTestDataWithDuplicates()
    writeInitialData(testData)
    writeWithErrorHandler(testData, "com.couchbase.spark.kv.TestLoggingErrorHandler")

    val logMessages = TestLoggingErrorHandler.getErrorInfos
    assertTrue(logMessages.nonEmpty, "No log messages captured")
    assertTrue(TestLoggingErrorHandler.getErrorCount > 0, "Log count should be greater than 0")

    val errorLog = logMessages.find(_.documentId == "doc1")
    assertTrue(errorLog.isDefined, "Should have error log for doc1")

    val errorInfos = TestLoggingErrorHandler.getErrorInfos
    assertTrue(errorInfos.nonEmpty, "No error info captured")
    assertTrue(TestLoggingErrorHandler.getErrorCount > 0, "Error count should be greater than 0")

    val doc1Error = errorInfos.find(_.documentId == "doc1")
    assertTrue(doc1Error.isDefined, "Should have error info for doc1")

    val errorInfo = doc1Error.get
    assertEquals(testResources.bucketName, errorInfo.bucket)
    assertEquals(testResources.scopeName, errorInfo.scope)
    assertEquals(testResources.collectionName, errorInfo.collection)
    assertEquals("doc1", errorInfo.documentId)
    assertTrue(errorInfo.throwable.isDefined)
    assertTrue(errorInfo.throwable.get.isInstanceOf[DocumentExistsException])

    TestLoggingErrorHandler.clear()
  }

  @Test
  def testFailFastErrorHandler(): Unit = {
    val testData = createTestDataWithDuplicates()
    writeInitialData(testData)

    try {
      writeWithErrorHandler(testData, "com.couchbase.spark.kv.FailFastErrorHandler")
      assert(false, "Expected exception but none was thrown")
    } catch {
      case _: Exception =>
    }
  }

  @Test
  def testErrorHandlerWithIgnoreMode(): Unit = {
    TestLoggingErrorHandler.clear()

    val testData = createTestDataWithDuplicates()
    writeInitialData(testData)
    writeWithErrorHandler(
      testData,
      "com.couchbase.spark.kv.TestLoggingErrorHandler",
      SaveMode.Ignore
    )

    val logMessages = TestLoggingErrorHandler.getErrorInfos
    assertTrue(
      logMessages.isEmpty,
      "No error handler calls expected for SaveMode.Ignore with DocumentExistsException"
    )
    assertEquals(
      0,
      TestLoggingErrorHandler.getErrorCount,
      "Error count should be 0 for SaveMode.Ignore"
    )

    TestLoggingErrorHandler.clear()
  }

  @Test
  def testInvalidErrorHandlerClass(): Unit = {
    val testData = createSimpleTestData()

    try {
      writeWithErrorHandler(testData, "com.invalid.NonExistentClass", SaveMode.Overwrite)
      assert(false, "Expected exception for invalid error handler class")
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains("Error handler class not found"))
    }

    try {
      writeWithErrorHandler(testData, "java.lang.Object", SaveMode.Overwrite)
      assert(false, "Expected exception for non-error-handler class")
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains("does not implement KeyValueWriteErrorHandler"))
    }

    try {
      writeWithErrorHandler(
        testData,
        "com.couchbase.spark.kv.NonSerializableErrorHandler",
        SaveMode.ErrorIfExists
      )
      assert(false, "Expected exception for non-serializable error handler")
    } catch {
      case e: Exception =>
        assert(
          e.getMessage.contains("Serialization") || e.getCause != null && e.getCause.getMessage
            .contains("Serialization")
        )
    }
  }

  @Test
  def testErrorHandlerWithUpsertMode(): Unit = {
    TestLoggingErrorHandler.clear()

    val testData = createSimpleTestData()
    writeWithErrorHandler(
      testData,
      "com.couchbase.spark.kv.TestLoggingErrorHandler",
      SaveMode.Overwrite
    )

    val logMessages = TestLoggingErrorHandler.getErrorInfos
    assertTrue(
      logMessages.isEmpty,
      "No error handler calls expected for successful upsert operations"
    )
    assertEquals(
      0,
      TestLoggingErrorHandler.getErrorCount,
      "Error count should be 0 for successful operations"
    )

    TestLoggingErrorHandler.clear()
  }

  @Test
  def testErrorDocumentsMode(): Unit = {
    val sp = spark

    val testData = createTestDataWithDuplicates()
    writeInitialData(testData)
    writeWithErrorDocuments(testData)

    val errorDocs = sp.read
      .format("couchbase.query")
      .option("bucket", testResources.bucketName)
      .option("scope", testResources.scopeName)
      .option("collection", ErrorsCollection)
      .load()
      .collect()

    assertTrue(errorDocs.nonEmpty, "No error documents found")

    val errorDoc = errorDocs.head
    assertTrue(
      errorDoc.schema.fieldNames.contains("timestamp"),
      "Error document missing timestamp field"
    )
    assertTrue(
      errorDoc.schema.fieldNames.contains("documentId"),
      "Error document missing documentId field"
    )
    assertTrue(errorDoc.schema.fieldNames.contains("bucket"), "Error document missing bucket field")
    assertTrue(errorDoc.schema.fieldNames.contains("scope"), "Error document missing scope field")
    assertTrue(
      errorDoc.schema.fieldNames.contains("collection"),
      "Error document missing collection field"
    )
    assertTrue(errorDoc.schema.fieldNames.contains("error"), "Error document missing error field")

    assertEquals(testResources.bucketName, errorDoc.getAs[String]("bucket"))
    assertEquals(testResources.scopeName, errorDoc.getAs[String]("scope"))
    assertEquals(testResources.collectionName, errorDoc.getAs[String]("collection"))

    val errorInfo = errorDoc.getAs[Row]("error")
    assertTrue(errorInfo.schema.fieldNames.contains("class"), "Error info missing class field")
    assertTrue(errorInfo.getAs[String]("class").contains("DocumentExistsException"))
  }

  @Test
  def testBothErrorHandlerAndErrorDocuments(): Unit = {
    TestLoggingErrorHandler.clear()

    val testData = createTestDataWithDuplicates()
    writeInitialData(testData)

    testData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.ErrorHandler, "com.couchbase.spark.kv.TestLoggingErrorHandler")
      .option(KeyValueOptions.ErrorBucket, testResources.bucketName)
      .option(KeyValueOptions.ErrorScope, testResources.scopeName)
      .option(KeyValueOptions.ErrorCollection, ErrorsCollection)
      .mode(SaveMode.ErrorIfExists)
      .save()

    val logMessages = TestLoggingErrorHandler.getErrorInfos
    assertTrue(logMessages.nonEmpty, "TestLoggingErrorHandler should have been called")

    val sp = spark

    val errorDocs = sp.read
      .format("couchbase.query")
      .option("bucket", testResources.bucketName)
      .option("scope", testResources.scopeName)
      .option("collection", ErrorsCollection)
      .load()
      .collect()

    assertTrue(errorDocs.nonEmpty, "Error documents should have been created")

    TestLoggingErrorHandler.clear()
  }

  override def testName: String = "CouchbaseWriteErrorHandlerIntegrationTest"
}
