package com.couchbase.spark

import com.couchbase.client.java.document.{RawJsonDocument, JsonDocument}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")

    val sc = new SparkContext(conf)

    // RDD will contain all found docs
    val docs = sc.couchbaseGet[JsonDocument](Seq("doc1", "doc2", "doc3"))

    // You can also customize the response document type from the SDK
    val doc = sc.couchbaseGet[RawJsonDocument]("rawDoc")

    println(docs
      .map(doc => (doc.id(), doc.content()))
      .first())
  }

}
