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
package com.couchbase.spark.query

import com.couchbase.spark.util.{SparkOperationalSimpleTest, TestNameUtil}
import org.apache.spark.sql.DataFrame
import org.junit.jupiter.api.Test

class QueryDataFrameWriteIntegrationTest extends SparkOperationalSimpleTest {
  override def testName: String = TestNameUtil.testName

  def basicReadQuery(): DataFrame = {
    spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .load()
  }

  @Test
  def testWriteDocumentsWithFilter(): Unit = {
    val airports = basicReadQuery()

    airports.write
      .format("couchbase.query")
      .option(QueryOptions.Scope, testResources.scopeName)
      .option(QueryOptions.Collection, testResources.collectionName)
      .mode("ignore")
      .save()
  }
}
