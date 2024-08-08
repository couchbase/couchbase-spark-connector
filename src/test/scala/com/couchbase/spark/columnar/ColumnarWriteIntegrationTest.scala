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

import com.couchbase.spark.columnar.util.ColumnarTestUtil.basicDataFrameReader
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.util.SparkColumnarTest
import org.apache.spark.sql.{AnalysisException, DataFrameWriter, Dataset, SaveMode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{BeforeAll, Disabled, Test}

// Note that write support has been disabled for now due to server limitations
@Disabled
class ColumnarWriteIntegrationTest extends SparkColumnarTest {
  override def testName: String = super.testName
  val saveDatabaseName          = "saveDatabase"
  val saveScopeName             = "saveScope"

  @BeforeAll
  def beforeAll(): Unit = {
    val cluster = CouchbaseConnection().cluster(CouchbaseConfig(spark.sparkContext.getConf))
    cluster.analyticsQuery(s"CREATE DATABASE ${saveDatabaseName}")
    cluster.analyticsQuery(s"CREATE SCOPE ${saveDatabaseName}.${saveScopeName}")
  }

  case class HelperResult(initial: Dataset[Airline], afterTest: Dataset[Airline])

  // todo failing with org.apache.spark.sql.AnalysisException: TableProvider implementation couchbase.columnar cannot be written with ErrorIfExists mode, please use Append or Overwrite modes instead.
  @Disabled
  @Test
  def withDefaults(): Unit = {
    val result = helper((airlines, saveCollectionName) => {
      basicWriteDataframe(airlines, saveCollectionName)
        .save()
    })

    assertEquals(result.initial.count(), result.afterTest.count())
  }

  @Test
  def appendMode(): Unit = {
    val result = helper((airlines, saveCollectionName) => {
      basicWriteDataframe(airlines, saveCollectionName)
        .mode(SaveMode.Append)
        .save()
    })

    assertEquals(result.initial.count(), result.afterTest.count())
  }

  // Not currently supported, to reduce initial scope.
  @Test
  def overwriteMode(): Unit = {
    // Expected: org.apache.spark.sql.AnalysisException: Table saveDatabase.saveScope.saveCollection does not support truncate in batch mode.
    assertThrows(
      classOf[AnalysisException],
      () =>
        helper((airlines, saveCollectionName) => {
          // Insert some data first so there is something to overwrite
          basicInsert(airlines, saveCollectionName)

          basicWriteDataframe(airlines, saveCollectionName)
            .mode(SaveMode.Overwrite)
            .save()
        })
    )
  }

  // Not currently supported, to reduce initial scope.
  @Test
  def ignoreMode(): Unit = {
    val sc = spark
    import sc.implicits._

    // Expected: org.apache.spark.sql.AnalysisException: TableProvider implementation couchbase.columnar cannot be written with Ignore mode, please use Append or Overwrite modes instead.
    assertThrows(
      classOf[AnalysisException],
      () =>
        helper((airlines, saveCollectionName) => {
          // Insert some data first so there is something to ignore
          basicInsert(airlines, saveCollectionName)

          val modified: Dataset[Airline] = airlines
            .map(v => v.copy(country = "England"))

          basicWriteDataframe(modified, saveCollectionName)
            .mode(SaveMode.Ignore)
            .save()
        })
    )
  }

  // Not currently supported, to reduce initial scope.
  @Test
  def errorIfExistsMode(): Unit = {
    // Expected: org.apache.spark.sql.AnalysisException: TableProvider implementation couchbase.columnar cannot be written with ErrorIfExists mode, please use Append or Overwrite modes instead.
    assertThrows(
      classOf[AnalysisException],
      () =>
        helper((airlines, saveCollectionName) => {
          // Insert some data first so there is something to fail if exists
          basicInsert(airlines, saveCollectionName)

          basicWriteDataframe(airlines, saveCollectionName)
            .mode(SaveMode.ErrorIfExists)
            .save()
        })
    )
  }

  // Not currently supported, to reduce initial scope.
  @Test
  def saveAsTable(): Unit = {
    // Expected: org.apache.spark.sql.AnalysisException: Table default.newtable does not support append in batch mode.;
    assertThrows(
      classOf[AnalysisException],
      () =>
        helper((airlines, saveCollectionName) => {
          basicWriteDataframe(airlines, saveCollectionName)
            .saveAsTable("newTable")
        })
    )
  }

  // Not currently supported, to reduce initial scope.
  @Test
  def insertInto(): Unit = {
    // Expected: org.apache.spark.sql.AnalysisException: Couldn't find a catalog to handle the identifier saveDatabase.saveScope.secondCollection.
    assertThrows(
      classOf[AnalysisException],
      () =>
        helper((airlines, saveCollectionName) => {
          val cluster = CouchbaseConnection().cluster(CouchbaseConfig(spark.sparkContext.getConf))
          cluster.analyticsQuery(
            s"CREATE COLLECTION ${saveDatabaseName}.${saveScopeName}.secondCollection PRIMARY KEY (id: string)"
          )

          basicWriteDataframe(airlines, saveCollectionName)
            .insertInto(s"`${saveDatabaseName}`.`${saveScopeName}`.`secondCollection`")
        }))
  }

  def basicWriteDataframe(airlines: Dataset[Airline], saveCollectionName: String): DataFrameWriter[Airline] = {
    airlines.write
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, saveDatabaseName)
      .option(ColumnarOptions.Scope, saveScopeName)
      .option(ColumnarOptions.Collection, saveCollectionName)
  }

  def basicInsert(airlines: Dataset[Airline], saveCollectionName: String): Unit = {
    airlines.write
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, saveDatabaseName)
      .option(ColumnarOptions.Scope, saveScopeName)
      .option(ColumnarOptions.Collection, saveCollectionName)
      .mode(SaveMode.Append)
      .save()
  }

  def helper(test: (Dataset[Airline], String) => Unit): HelperResult = {
    val sc = spark
    import sc.implicits._

    val airlines = basicDataFrameReader(spark)
      .load()
      .as[Airline]
      .filter(airline => airline.country == "France")

    val cluster = CouchbaseConnection().cluster(CouchbaseConfig(spark.sparkContext.getConf))
    val saveCollectionName = testName

    cluster.analyticsQuery(
      s"CREATE COLLECTION ${saveDatabaseName}.${saveScopeName}.${saveCollectionName} PRIMARY KEY (id: string)"
    )

    test(airlines, saveCollectionName)
//      airlines.write
//        .format("couchbase.columnar")
//        .option(ColumnarOptions.Database, saveDatabaseName)
//        .option(ColumnarOptions.Scope, saveScopeName)
//        .option(ColumnarOptions.Collection, saveCollectionName)
//        .mode(SaveMode.Append)
//        .save()
//
//    // todo test insertInto
//      // todo test all SaveModes

    val reread = spark.read
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, saveDatabaseName)
      .option(ColumnarOptions.Scope, saveScopeName)
      .option(ColumnarOptions.Collection, saveCollectionName)
      .load()
      .as[Airline]

    HelperResult(airlines, reread)
  }
}
