package com.couchbase.spark

import java.util.concurrent.TimeUnit

import com.couchbase.client.java.document.{RawJsonDocument, JsonArrayDocument, JsonDocument}
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.connection.{CouchbaseConnection, CouchbaseConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import com.couchbase.spark._

object Main {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      // spark specific params
      .setMaster("local[*]")
      .setAppName("myapp")

    // Start your spark context
    val sc = new SparkContext(conf)

val docs = sc
  .couchbaseView(query = ViewQuery.from("user", "all").limit(100).descending().reduce(false))
  .map(_.id)
  .couchbaseGet[JsonDocument]()
  .collect()

  }

}
