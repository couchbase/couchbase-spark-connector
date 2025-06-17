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

import com.couchbase.client.core.error.CasMismatchException
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.query.QueryOptions
import com.couchbase.spark.util.SparkOperationalSimpleTest
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class ReplaceCasDataFrameIntegrationTest extends SparkOperationalSimpleTest {

  override def testName: String = "ReplaceCasDataFrameIntegrationTest"

  private def readDataFrameWithCas() = {
    spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, testResources.bucketName)
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .option(QueryOptions.OutputCas, "true")
      .load()
  }

  private def readDataFrameWithoutCas() = {
    spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, testResources.bucketName)
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .load()
  }

  private def createTestDataWithoutCas() = {
    val sp = spark
    import sp.implicits._

    Seq(
      ("doc1", "initial_value1"),
      ("doc2", "initial_value2")
    ).toDF("__META_ID", "content")
  }

  private def writeDataFrame(df: org.apache.spark.sql.DataFrame, mode: SaveMode = SaveMode.Overwrite) = {
    df.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .mode(mode)
      .save()
  }

  @Test
  def testReplaceWithValidCas(): Unit = {
    val initialData = createTestDataWithoutCas()
    writeDataFrame(initialData)

    val readWithCas = readDataFrameWithCas()
    val casData = readWithCas.collect()
    assertTrue(casData.length >= 2, "Should have at least 2 documents")

    val updatedData = readWithCas.withColumn("content", lit("updated"))

    updatedData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
      .option(KeyValueOptions.CasFieldName, DefaultConstants.DefaultCasFieldName)
      .save()

    val finalRead = readDataFrameWithoutCas()
    val finalData = finalRead.collect()
    assertTrue(finalData.exists(_.getAs[String]("content") == "updated"),
      "Should have updated content")
  }

  @Test
  def testReplaceWithInvalidCasErrorHandler(): Unit = {
    val initialData = createTestDataWithoutCas()
    writeDataFrame(initialData)

    val readWithCas = readDataFrameWithCas()

    val updatedData = readWithCas
      .withColumn("content", lit("updated"))
      // Fake a CAS mismatch because trying to induce a real one in-between read and write is very challenging with
      // how Spark DataFrames work
      .withColumn("__META_CAS", col("__META_CAS") + 99999)

    TestLoggingErrorHandler.clear()
    updatedData.printSchema()

    updatedData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
      .option(KeyValueOptions.CasFieldName, DefaultConstants.DefaultCasFieldName)
      .option(KeyValueOptions.ErrorHandler, "com.couchbase.spark.kv.TestLoggingErrorHandler")
      .save()

    val errors = TestLoggingErrorHandler.getErrorInfos
    assertTrue(errors.nonEmpty, "Should have captured CAS mismatch errors")
    assertTrue(errors.exists(_.throwable.exists(_.isInstanceOf[CasMismatchException])), 
      "Should have CasMismatchException errors")
  }

  @Test
  def testReplaceWithInvalidCas(): Unit = {
    val initialData = createTestDataWithoutCas()
    writeDataFrame(initialData)

    val readWithCas = readDataFrameWithCas()

    val updatedData = readWithCas
      .withColumn("content", lit("updated"))
      .withColumn("__META_CAS", col("__META_CAS") + 99999)

    assertThrows(classOf[Exception], () => {
      updatedData.write
        .format("couchbase.kv")
        .option(KeyValueOptions.Bucket, testResources.bucketName)
        .option(KeyValueOptions.Scope, testResources.scopeName)
        .option(KeyValueOptions.Collection, testResources.collectionName)
        .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
        .option(KeyValueOptions.CasFieldName, DefaultConstants.DefaultCasFieldName)
        .save()
    }, "Should throw exception when CAS mismatch occurs without error handler")
  }

  @Test
  def testReplaceWithoutCasFieldIgnoresCas(): Unit = {
    val initialData = createTestDataWithoutCas()
    writeDataFrame(initialData)

    val dataWithCas = readDataFrameWithCas()

    val updatedData = dataWithCas
      .withColumn("content", lit("updated"))
      .withColumn("__META_CAS", col("__META_CAS") + 99999)

    updatedData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
      .save()

    val finalRead = readDataFrameWithoutCas()
    val finalData = finalRead.collect()
    assertTrue(finalData.exists(_.getAs[String]("content").contains("updated")),
      "Should have updated content, proving CAS was ignored")
  }

  @Test
  def testReplaceWithExplicitCasFieldFailsWithoutCas(): Unit = {
    val initialData = createTestDataWithoutCas()
    writeDataFrame(initialData)

    val dataWithoutCas = readDataFrameWithoutCas()

    assertThrows(classOf[Exception], () => {
      dataWithoutCas.write
        .format("couchbase.kv")
        .option(KeyValueOptions.Bucket, testResources.bucketName)
        .option(KeyValueOptions.Scope, testResources.scopeName)
        .option(KeyValueOptions.Collection, testResources.collectionName)
        .option(KeyValueOptions.CasFieldName, "custom_cas_field")
        .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
        .save()
    }, "Should throw exception when using explicit CAS field name without CAS values")
  }

  @Test
  def testReplaceWithExplicitCasFieldFailsWithoutCasEvenWithErrorHandler(): Unit = {
    val initialData = createTestDataWithoutCas()
    writeDataFrame(initialData)

    val sp = spark
    import sp.implicits._

    val dataWithoutCas = Seq(
      ("doc1", "should_fail_without_cas1"),
      ("doc2", "should_fail_without_cas2")
    ).toDF("__META_ID", "content")

    TestLoggingErrorHandler.clear()

    assertThrows(classOf[Exception], () => {
      dataWithoutCas.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.CasFieldName, "custom_cas_field")
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
      .option(KeyValueOptions.ErrorHandler, "com.couchbase.spark.kv.TestLoggingErrorHandler")
      .save()
    }, "Should throw exception when using explicit CAS field name without CAS values, even when using ErrorHandler")
  }
}