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
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query._
import com.couchbase.client.java.view.{DefaultView, DesignDocument, Stale, ViewQuery}
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import com.couchbase.spark._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import scala.Console.in

/**
 * Integration test to verify Spark Context functionality in combination with Couchbase Server
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
@RunWith(classOf[JUnitRunner])
class SparkContextFunctionsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local[2]"
  private val appName = "cb-int-specs1"
  private val bucketName = "default"

  private var sparkContext: SparkContext = null
  private var bucket: Bucket = null

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.couchbase.nodes", "127.0.0.1")
      .set("spark.couchbase.username", "Administrator")
      .set("spark.couchbase.password", "password")
      .set("com.couchbase.bucket." + bucketName, "")
     val spark = SparkSession.builder()
       .config(conf)
       .getOrCreate()
    sparkContext = spark.sparkContext
    bucket = CouchbaseConnection().bucket(CouchbaseConfig(conf), bucketName)
    bucket.bucketManager().flush()

    val ddoc = DesignDocument.create("spark_design", List(DefaultView.create("view",
      "function (doc, meta) { if (doc.type == \"user\") { emit(doc.username, null); } }"))
    )
    bucket.bucketManager().upsertDesignDocument(ddoc)

    bucket.query(N1qlQuery.simple(s"CREATE PRIMARY INDEX ON $bucketName"))
  }

  override def afterAll(): Unit = {
    CouchbaseConnection().stop()
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
      JsonDocument.create("user-1", JsonObject.create()
        .put("type", "user").put("username", "Michael"))
    )
    bucket.upsert(
      JsonDocument.create("user-2", JsonObject.create()
        .put("type", "user").put("username", "Simon"))
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
      JsonDocument.create("car-1", JsonObject.create()
        .put("type", "car").put("name", "Ford Mustang"))
    )

    bucket.upsert(
      JsonDocument.create("car-2", JsonObject.create().put("type", "car").put("name", "VW Passat"))
    )

    val result = sparkContext
      .couchbaseQuery(
        N1qlQuery.simple(
          s"""SELECT META(`$bucketName`).id,
              | `$bucketName`.*
              | FROM `$bucketName`
              |  WHERE type = 'car'""".stripMargin,
          N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS))
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
