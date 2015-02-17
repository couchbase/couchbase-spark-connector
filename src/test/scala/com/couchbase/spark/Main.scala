package com.couchbase.spark

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.document.{Document, RawJsonDocument, JsonArrayDocument, JsonDocument}
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.consistency.ScanConsistency
import com.couchbase.client.java.query.{Select, QueryParams, Query}
import com.couchbase.client.java.view.{SpatialViewQuery, ViewQuery}
import com.couchbase.spark.connection.{CouchbaseConnection, CouchbaseConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import com.couchbase.spark._

object Main {

  def main(args: Array[String]) {

    System.setProperty("com.couchbase.queryEnabled", "true")

    val conf = new SparkConf()
      // spark specific params
      .set("com.couchbase.bucket.default", "")
      .setMaster("local[*]")
      .setAppName("myapp")

    // Start your spark context
    val sc = new SparkContext(conf)




    val doc1 = ("doc1", Map("key" -> "value"))
    val doc2 = ("doc2", Map("a" -> 1, "b" -> true))

    val data = sc
      .parallelize(Seq(doc1, doc2))
      .toCouchbaseDocument[JsonDocument]
      .saveToCouchbase()

  }

}
