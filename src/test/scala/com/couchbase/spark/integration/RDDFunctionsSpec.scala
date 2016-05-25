/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.spark.integration

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.query.core.N1qlQueryExecutor
import com.couchbase.client.java.view.{SpatialViewQuery, ViewQuery}
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import com.couchbase.spark._

/**
 * Integration test to verify RDD functionality in combination with Couchbase Serve
 * r
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
class RDDFunctionsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val master = "local[2]"
  private val appName = "cb-int-specs2"
  private val bucketName = "default"

  private var sparkContext: SparkContext = null
  private var bucket: Bucket = null

  override def beforeAll() {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sparkContext = new SparkContext(conf)
    bucket = CouchbaseConnection().bucket(CouchbaseConfig(conf), bucketName)
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A RDD" should "be created as a transformation" in {
    bucket.upsert(JsonDocument.create("doc1", JsonObject.create().put("val", "doc1")))
    bucket.upsert(JsonDocument.create("doc2", JsonObject.create().put("val", "doc2")))
    bucket.upsert(JsonDocument.create("doc3", JsonObject.create().put("val", "doc3")))


    val result = sparkContext
      .parallelize(Seq("doc1", "doc2", "doc3"))
      .couchbaseGet[JsonDocument]()
      .collect()

    result should have size 3
    result.foreach { doc =>
      doc.content().getString("val") should equal (doc.id())
    }
  }

}
