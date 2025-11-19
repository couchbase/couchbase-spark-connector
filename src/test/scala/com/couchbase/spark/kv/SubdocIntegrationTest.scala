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
import com.couchbase.spark.{Keyspace, util, DefaultConstants}
import com.couchbase.spark.util.SparkOperationalSimpleTest
import com.couchbase.spark.query.QueryOptions
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeAll, Test}

class SubdocIntegrationTest extends SparkOperationalSimpleTest {
  private val TestCollection = "subdoc_test"

  override def testName: String = "subdoc"

  @BeforeAll
  def setUpCollection(): Unit = {
    testResourceCreator.createCollection(
      testResources.bucketName,
      testResources.scopeName,
      Some(TestCollection)
    )
  }

  @Test
  def testReadFromQueryWriteToKV(): Unit = {
    val sp = spark
    import sp.implicits._

    // First, create some source data in the collection
    val sourceData = Seq(
      ("query_doc1", "John", 25, "Engineer"),
      ("query_doc2", "Jane", 30, "Manager"),
      ("query_doc3", "Bob", 35, "Developer")
    ).toDF("__META_ID", "name", "age", "role")

    sourceData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, TestCollection)
      .mode(SaveMode.Overwrite)
      .save()

    // Read data from query
    val queryData = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, TestCollection)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    // Transform the data and prepare for subdoc operations
    val transformedData = queryData
      .select(
        queryData("__META_ID"),
        queryData("name"),
        queryData("age"),
        queryData("role")
      )
      .withColumnRenamed("name", "upsert:profile.name")
      .withColumnRenamed("age", "upsert:profile.age")
      .withColumnRenamed("role", "upsert:profile.role")

    // Write the transformed data back to KV using subdoc upsert
    transformedData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, TestCollection)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeSubdocUpsert)
      .save()

    // Verify the data was written correctly
    import com.couchbase.spark._

    val keyspace = Keyspace(
      bucket = Some(testResources.bucketName),
      scope = Some(testResources.scopeName),
      collection = Some(TestCollection)
    )

    val results = spark.sparkContext
      .couchbaseGet(Seq(Get("query_doc1"), Get("query_doc2"), Get("query_doc3")), keyspace)
      .collect()

    assertEquals(3, results.length)

    // Verify the structure of the documents
    val doc1 = results.find(_.id == "query_doc1").get.contentAs[JsonObject].get
    val doc2 = results.find(_.id == "query_doc2").get.contentAs[JsonObject].get
    val doc3 = results.find(_.id == "query_doc3").get.contentAs[JsonObject].get

    // Check that the profile subdocument was created correctly
    assertEquals("John", doc1.obj("profile").str("name"))
    assertEquals(25, doc1.obj("profile").num("age"))
    assertEquals("Engineer", doc1.obj("profile").str("role"))

    assertEquals("Jane", doc2.obj("profile").str("name"))
    assertEquals(30, doc2.obj("profile").num("age"))
    assertEquals("Manager", doc2.obj("profile").str("role"))

    assertEquals("Bob", doc3.obj("profile").str("name"))
    assertEquals(35, doc3.obj("profile").num("age"))
    assertEquals("Developer", doc3.obj("profile").str("role"))
  }

  @Test
  def operationsOnNewDoc(): Unit = {
    val sp = spark
    import sp.implicits._

    val sourceData = Seq(
      ("docA", 1, "foo", 10.0),
      ("docB", 2, "bar", 20.0),
      ("docC", 3, "baz", 30.3)
    ).toDF("__META_ID", "upsert:profile.version", "insert:name", "upsert:score")

    sourceData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeSubdocUpsert)
      .save()

    import com.couchbase.spark._

    val keyspace = Keyspace(
      bucket = Some(testResources.bucketName),
      scope = Some(testResources.scopeName),
      collection = Some(testResources.collectionName)
    )

    val results = spark.sparkContext
      .couchbaseGet(Seq(Get("docC")), keyspace)
      .collect()

    assertEquals(1, results.length)

    val doc = results.head.contentAs[JsonObject].get

    assertEquals("baz", doc.str("name"))
    assertEquals(30.3, doc.numFloat("score"), 0.01)
    assertEquals(3, doc.obj("profile").num("version"))
  }

  @Test
  def operationsOnExistingDoc(): Unit = {
    val sp = spark
    import sp.implicits._
    val setupData = Seq(
      ("removeDoc", "initialValue", 100, 50, "keep", Seq("tag1", "tag2"), Seq("tagA", "tagD"), Map("foo"-> Map("baz" -> "bux")))
    ).toDF("__META_ID", "upsert:toRemove", "upsert:counter1", "upsert:counter2", "upsert:keepThis", "upsert:tags", "upsert:moreTags", "nestedJson")

    setupData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeSubdocUpsert)
      .save()

    val removeData = Seq(
      // Note Spark removes null fields, so need to use something like "" for the toRemove value
      ("removeDoc", "", 1, 2, "tag0", "tag3", Seq("tagB", "tagC"), "bux2")
    ).toDF(
      "__META_ID",
      "remove:toRemove",
      "decrement:counter1",
      "increment:counter2",
      "arrayPrepend:tags",
      "arrayAppend:tags",
      "arrayInsert:moreTags[1]",
      "nestedJson.foo.baz"
    )

    removeData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, testResources.collectionName)
      // Need to use replace as we're removing a field
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeSubdocReplace)
      .save()

    import com.couchbase.spark._

    val keyspace = Keyspace(
      bucket = Some(testResources.bucketName),
      scope = Some(testResources.scopeName),
      collection = Some(testResources.collectionName)
    )

    val results = spark.sparkContext
      .couchbaseGet(Seq(Get("removeDoc")), keyspace)
      .collect()

    assertEquals(1, results.length)

    val doc = results.head.contentAs[JsonObject].get

    assertFalse(doc.names.contains("toRemove"))
    assertEquals("keep", doc.str("keepThis"))
    val tags = doc.arr("tags")
    assertEquals(4, tags.size)
    assertEquals("tag0", tags.str(0))
    assertEquals("tag1", tags.str(1))
    assertEquals("tag2", tags.str(2))
    assertEquals("tag3", tags.str(3))

    val moreTags = doc.arr("moreTags")
    assertEquals(4, moreTags.size)
    assertEquals("tagA", moreTags.str(0))
    assertEquals("tagB", moreTags.str(1))
    assertEquals("tagC", moreTags.str(2))
    assertEquals("tagD", moreTags.str(3))
    assertEquals(99, doc.num("counter1"))
    assertEquals(52, doc.num("counter2"))
    assertEquals("bux2", doc.obj("nestedJson").obj("foo").str("baz"))
  }

  @Test
  def testSubdocReplaceWithCas(): Unit = {
    val sp = spark
    import sp.implicits._

    // First, create some initial documents with nested structure
    val initialData = Seq(
      ("cas_doc1", "John", 25, "Engineer"),
      ("cas_doc2", "Jane", 30, "Manager"),
      ("cas_doc3", "Bob", 35, "Developer")
    ).toDF("__META_ID", "name", "age", "role")

    // Create initial documents with nested structure using subdoc upsert
    val initialDataWithNested = initialData
      .withColumnRenamed("name", "upsert:profile.name")
      .withColumnRenamed("age", "upsert:profile.age")
      .withColumnRenamed("role", "upsert:profile.role")

    initialDataWithNested.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Timeout, "30s")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, TestCollection)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeSubdocUpsert)
      .mode(SaveMode.Overwrite)
      .save()

    // Read the documents with CAS values included
    val docsWithCas = spark.read
      .format("couchbase.query")
      .option(KeyValueOptions.Timeout, "30s")
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, TestCollection)
      .option(QueryOptions.OutputCas, "true")
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    // Transform the data for subdoc replace operations with CAS
    val transformedData = docsWithCas
      .select(
        docsWithCas("__META_ID"),
        docsWithCas("__META_CAS"),
        docsWithCas("profile.name"),
        docsWithCas("profile.age"),
        docsWithCas("profile.role")
      )
      .withColumnRenamed("profile.name", "replace:profile.name")
      .withColumnRenamed("profile.age", "replace:profile.age")
      .withColumnRenamed("profile.role", "replace:profile.role")

    // Write the transformed data back using subdoc replace with CAS
    transformedData.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Timeout, "30s")
      .option(KeyValueOptions.Bucket, testResources.bucketName)
      .option(KeyValueOptions.Scope, testResources.scopeName)
      .option(KeyValueOptions.Collection, TestCollection)
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeSubdocReplace)
      .option(KeyValueOptions.CasFieldName, DefaultConstants.DefaultCasFieldName)
      .save()

    // Verify the data was written correctly
    import com.couchbase.spark._

    val keyspace = Keyspace(
      bucket = Some(testResources.bucketName),
      scope = Some(testResources.scopeName),
      collection = Some(TestCollection)
    )

    val results = spark.sparkContext
      .couchbaseGet(Seq(Get("cas_doc1"), Get("cas_doc2"), Get("cas_doc3")), keyspace)
      .collect()

    assertEquals(3, results.length)

    // Verify the structure of the documents
    val doc1 = results.find(_.id == "cas_doc1").get.contentAs[JsonObject].get
    val doc2 = results.find(_.id == "cas_doc2").get.contentAs[JsonObject].get
    val doc3 = results.find(_.id == "cas_doc3").get.contentAs[JsonObject].get

    // Check that the profile subdocument was created correctly
    assertEquals("John", doc1.obj("profile").str("name"))
    assertEquals(25, doc1.obj("profile").num("age"))
    assertEquals("Engineer", doc1.obj("profile").str("role"))

    assertEquals("Jane", doc2.obj("profile").str("name"))
    assertEquals(30, doc2.obj("profile").num("age"))
    assertEquals("Manager", doc2.obj("profile").str("role"))

    assertEquals("Bob", doc3.obj("profile").str("name"))
    assertEquals(35, doc3.obj("profile").num("age"))
    assertEquals("Developer", doc3.obj("profile").str("role"))

    // Verify that CAS values are present and valid
    results.foreach(result => {
      assertTrue(result.cas > 0, s"CAS value should be positive for document ${result.id}, got: ${result.cas}")
    })
  }
}
