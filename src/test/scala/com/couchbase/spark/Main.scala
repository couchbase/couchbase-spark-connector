package com.couchbase.spark

import com.couchbase.client.java.document.JsonDocument
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val docs = sc.couchbaseGet[JsonDocument]("foo")

    println(docs
      .map(doc => (doc.id(), doc.content()))
      .first())
  }

}
