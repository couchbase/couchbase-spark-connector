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

object Main {

  def main(args: Array[String]) {

val conf = new SparkConf()
  // spark specific params
  .setMaster("local[*]")
  .setAppName("myapp")
  // couchbase specific params
  .set("com.couchbase.nodes", "192.168.56.101;192.168.56.102")
  .set("com.couchbase.bucket.mybucket", "password")

// Start your spark context
val sc = new SparkContext(conf)


    sc.couchbaseGet[JsonDocument](ids = Seq("doc")).collect()

  }

}
