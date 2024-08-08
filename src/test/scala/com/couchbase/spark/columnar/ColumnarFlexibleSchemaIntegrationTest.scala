/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.spark.columnar

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark._
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.util.SparkColumnarTest
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.{BeforeAll, Test}

case class User(id: String, username: String, age: Int)
case class Order(id: String, value: Double)
case class Basket(id: String, itemIds: Seq[String])
case class ItemWithoutId(missing: Double)

class ColumnarFlexibleSchemaIntegrationTest extends SparkColumnarTest {
  override def testName: String  = super.testName
  val saveDatabaseName           = "saveDatabase"
  val saveScopeName              = "saveScope"
  val saveCollectionName: String = testName

  @BeforeAll
  def beforeAll(): Unit = {
    val cluster      = CouchbaseConnection().cluster(CouchbaseConfig(spark.sparkContext.getConf))
    val sparkSession = spark
    import sparkSession.implicits._

    cluster.analyticsQuery(s"CREATE DATABASE ${saveDatabaseName}")
    cluster.analyticsQuery(s"CREATE SCOPE ${saveDatabaseName}.${saveScopeName}")

    cluster.analyticsQuery(
      s"CREATE COLLECTION ${saveDatabaseName}.${saveScopeName}.${saveCollectionName} PRIMARY KEY (id: string)"
    )

    val users             = Seq(User("andy", "Andy", 32), User("beth", "Beth", 26)).toDS()
    val orders            = Seq(Order("order1", 2.67), Order("order2", 1.92)).toDS()
    val baskets           = Seq(Basket("basket1", Seq("order1", "order2"))).toDS()
    val itemsWithoutIdCol = Seq(ItemWithoutId(20)).toDS()

    users.write
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, saveDatabaseName)
      .option(ColumnarOptions.Scope, saveScopeName)
      .option(ColumnarOptions.Collection, saveCollectionName)
      .mode(SaveMode.Append)
      .save()

    orders.write
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, saveDatabaseName)
      .option(ColumnarOptions.Scope, saveScopeName)
      .option(ColumnarOptions.Collection, saveCollectionName)
      .mode(SaveMode.Append)
      .save()

    baskets.write
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, saveDatabaseName)
      .option(ColumnarOptions.Scope, saveScopeName)
      .option(ColumnarOptions.Collection, saveCollectionName)
      .mode(SaveMode.Append)
      .save()

    // Note these will silently be discarded by the database
    itemsWithoutIdCol.write
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, saveDatabaseName)
      .option(ColumnarOptions.Scope, saveScopeName)
      .option(ColumnarOptions.Collection, saveCollectionName)
      .mode(SaveMode.Append)
      .save()
  }

  // With defaults, inference will find all docs in this collection and produce an 'uber-schema' that's the union of
  // all the schemas.  Where rows don't have a matching value for a column, it's filled with nulls.
  @Test
  def mixedSchema(): Unit = {
    val docs = spark.read
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, saveDatabaseName)
      .option(ColumnarOptions.Scope, saveScopeName)
      .option(ColumnarOptions.Collection, saveCollectionName)
      .load()

    val schemaSeq = docs.schema.toSeq
    assertTrue(schemaSeq.exists(v => v.name == "id"))
    assertTrue(schemaSeq.exists(v => v.name == "username"))
    assertTrue(schemaSeq.exists(v => v.name == "age"))
    assertTrue(schemaSeq.exists(v => v.name == "value"))
    assertTrue(schemaSeq.exists(v => v.name == "itemIds"))
    // As these are discarded by the database
    assertFalse(schemaSeq.exists(v => v.name == "missing"))
  }

  // With such a small infer limit Spark will pick a schema based on the first document it gets back.  All docs will
  // be squished into this schema.  This generally means losing a bunch of columns/fields.
  @Test
  def smallInferLimit(): Unit = {
    val docs = spark.read
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, "travel-sample")
      .option(ColumnarOptions.Scope, "inventory")
      .option(ColumnarOptions.Collection, "iarline")
      .load()

    // The only thing we can safely assert is this.  The other fields will depend on which document was used for inference
    val schemaSeq = docs.schema.toSeq
    assertTrue(schemaSeq.exists(v => v.name == "id"))
  }
}
