package com.couchbase.spark

import com.couchbase.client.java.document.{JsonArrayDocument, JsonDocument}
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.view.ViewQuery
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("couchbase.bucket", "default")
      .setMaster("local")
      .setAppName("test")

    val sc = new SparkContext(conf)

    val simple = sc
      .parallelize(Seq(("id", JsonObject.create().put("foo", "bar"))))
      .toCouchbaseDocument
      .saveToCouchbase()

    val arr = sc
      .parallelize(Seq(("arr", JsonArray.from("a", "b"))))
      .toCouchbaseDocument
      .saveToCouchbase()

    val direct =
      sc.parallelize(Seq(JsonDocument.create("do", JsonObject.empty()), JsonArrayDocument.create("da", JsonArray.empty())))
      .saveToCouchbase()
  }

}
