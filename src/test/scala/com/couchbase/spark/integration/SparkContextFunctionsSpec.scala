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
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.{QueryParams, Query, SimpleQuery}
import com.couchbase.client.java.view.{Stale, ViewQuery, DefaultView, DesignDocument}
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import scala.collection.JavaConversions._

import com.couchbase.spark._

/**
 * Integration test to verify Spark Context functionality in combination with Couchbase Server
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
class SparkContextFunctionsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local[2]"
  private val appName = "cb-int-specs1"
  private val bucketName = "default"

  private var sparkContext: SparkContext = null
  private var bucket: Bucket = null

  override def beforeAll() {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sparkContext = new SparkContext(conf)
    bucket = CouchbaseConnection().bucket(CouchbaseConfig(conf), bucketName)

    val ddoc = DesignDocument.create("spark_design", List(DefaultView.create("view",
      "function (doc, meta) { if (doc.type == \"user\") { emit(doc.username, null); } }"))
    )
    bucket.bucketManager().upsertDesignDocument(ddoc)

    bucket.query(Query.simple(s"CREATE PRIMARY INDEX ON $bucketName"))
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A RDD" should "be created from Couchbase Document IDs" in {

    bucket.upsert(JsonDocument.create("doc1", JsonObject.create().put("val", "doc1")))
    bucket.upsert(JsonDocument.create("doc2", JsonObject.create().put("val", "doc2")))
    bucket.upsert(JsonDocument.create("doc3", JsonObject.create().put("val", "doc3")))

    val result = sparkContext
      .couchbaseGet[JsonDocument](Seq("doc1", "doc2", "doc3", "doesNotExist"))
      .collect()

    result should have size 3
    result.foreach { doc =>
      doc.content().getString("val") should equal (doc.id())
    }
  }

  it should "be created from a View" in {

    bucket.upsert(
      JsonDocument.create("user-1", JsonObject.create().put("type", "user").put("username", "Michael"))
    )
    bucket.upsert(
      JsonDocument.create("user-2", JsonObject.create().put("type", "user").put("username", "Simon"))
    )

    val result = sparkContext
      .couchbaseView(ViewQuery.from("spark_design", "view").stale(Stale.FALSE))
      .collect()

    result should have size 2
    result.foreach { doc =>
      if (doc.id == "user-1") {
        doc.key should equal ("Michael")
      } else if (doc.id == "user-2") {
        doc.key should equal ("Simon")
      }
    }
  }

  it should "be created from a N1QL Query" in {
    bucket.upsert(
      JsonDocument.create("car-1", JsonObject.create().put("type", "car").put("name", "Ford Mustang"))
    )

    bucket.upsert(
      JsonDocument.create("car-2", JsonObject.create().put("type", "car").put("name", "VW Passat"))
    )

    val result = sparkContext
      .couchbaseQuery(
          Query.simple(s"SELECT META(`$bucketName`).id, `$bucketName`.* FROM `$bucketName` WHERE type = 'car'",
          QueryParams.build().consistency(ScanConsistency.REQUEST_PLUS))
      )
      .collect()

    result should have size 2
    result.foreach { doc =>
      if (doc.value.getString("id") == "car-1") {
        doc.value.getString("name") should equal ("Ford Mustang")
      } else if (doc.value.getString("id") == "car-1") {
        doc.value.getString("name") should equal ("VW Passat")
      }
    }

  }

}
