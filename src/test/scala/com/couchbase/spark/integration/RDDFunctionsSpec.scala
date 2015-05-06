/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark.integration

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterAll}

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
