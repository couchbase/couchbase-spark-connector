package com.couchbase.spark

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.document.{RawJsonDocument, JsonArrayDocument, JsonDocument}
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
      .set("com.couchbase.bucket.beer-sample", "")
      .setMaster("local[*]")
      .setAppName("myapp")

    // Start your spark context
    val sc = new SparkContext(conf)

val docs = sc
  .couchbaseQuery(query = Query.simple(Select.select("count(*) as cnt").from("`beer-sample`"), QueryParams.build().consistency(ScanConsistency.NOT_BOUNDED)))
    .map(row => row.value.getInt("cnt"))
    .foreach(println)

  }

}
