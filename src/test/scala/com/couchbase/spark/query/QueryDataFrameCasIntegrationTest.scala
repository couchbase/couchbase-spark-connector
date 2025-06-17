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
package com.couchbase.spark.query

import com.couchbase.spark.kv.KeyValueOptions
import com.couchbase.spark.util.SparkOperationalSimpleTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class QueryDataFrameCasIntegrationTest extends SparkOperationalSimpleTest {
  override def testName: String = "QueryDataFrameCasIntegrationTest"

  private def createTestData() = {
    val sp = spark
    import sp.implicits._

    Seq(
      ("cas_test_doc1", "test_value1"),
      ("cas_test_doc2", "test_value2"),
      ("cas_test_doc3", "test_value3")
    ).toDF("__META_ID", "content")
  }

  def setupTestDocuments(): Unit = {
    val testData = createTestData()
    testData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .mode(SaveMode.Overwrite)
      .save()
  }

  @Test
  def testQueryWithOutputCasTrue(): Unit = {
    setupTestDocuments()

    val df = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, testResources.bucketName)
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .option(QueryOptions.OutputCas, "true")
      .load()

    val result = df.collect()
    assertTrue(result.nonEmpty, "Should have results")

    val schema = df.schema
    assertTrue(schema.fieldNames.contains("__META_CAS"), "Schema should contain __META_CAS field")

    result.foreach(row => {
      val cas = row.getAs[Long]("__META_CAS")
      assertTrue(cas > 0, s"CAS value should be positive, got: $cas")
    })
  }

  @Test
  def testQueryWithOutputCasFalse(): Unit = {
    setupTestDocuments()

    val df = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, testResources.bucketName)
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .option(QueryOptions.OutputCas, "false")
      .load()

    val schema = df.schema
    assertFalse(schema.fieldNames.contains("__META_CAS"), "Schema should not contain __META_CAS field")
  }

  @Test
  def testQueryWithOutputCasDefault(): Unit = {
    setupTestDocuments()

    val df = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, testResources.bucketName)
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .load()

    val schema = df.schema
    assertFalse(schema.fieldNames.contains("__META_CAS"), "Schema should not contain __META_CAS field by default")
  }

  @Test
  def testQueryWithCustomCasFieldName(): Unit = {
    setupTestDocuments()

    val customCasFieldName = "custom_cas_field"

    val df = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, testResources.bucketName)
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .option(QueryOptions.OutputCas, "true")
      .option(QueryOptions.CasFieldName, customCasFieldName)
      .load()

    val schema = df.schema
    assertTrue(schema.fieldNames.contains(customCasFieldName), s"Schema should contain $customCasFieldName field")
    assertFalse(schema.fieldNames.contains("__META_CAS"), "Schema should not contain default __META_CAS field")

    val result = df.collect()
    assertTrue(result.nonEmpty, "Should have results")

    result.foreach(row => {
      val cas = row.getAs[Long](customCasFieldName)
      assertTrue(cas > 0, s"CAS value should be positive, got: $cas")
    })
  }

  @Test
  def testQueryWithCasFieldNameButOutputCasFalse(): Unit = {
    setupTestDocuments()

    val customCasFieldName = "custom_cas_field"

    val df = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Bucket, testResources.bucketName)
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .option(QueryOptions.OutputCas, "false")
      .option(QueryOptions.CasFieldName, customCasFieldName)
      .load()

    val schema = df.schema
    assertFalse(schema.fieldNames.contains(customCasFieldName), s"Schema should not contain $customCasFieldName field when OutputCas is false")
    assertFalse(schema.fieldNames.contains("__META_CAS"), "Schema should not contain default __META_CAS field")
  }
}