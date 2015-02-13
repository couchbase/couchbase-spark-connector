package com.couchbase.spark

import com.couchbase.client.java.document.{RawJsonDocument, JsonDocument}
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("couchbase.bucket", "beer-sample")
      .setMaster("local")
      .setAppName("test")

    val sc = new SparkContext(conf)

    // Use ids in the codes
    val docs = sc.couchbaseGet("id")
    val docs1 = sc.couchbaseGet("id1", "id2")
    val ids = List("id1", "id2")
    val docs3 = sc.couchbaseGet(ids: _*)

    // or read ids from files
    val docs4 = sc.textFile("this_is_a_path").documents

    val allDocsStartingWithNameA = sc
      .couchbaseView(ViewQuery.from("beer", "brewery_beers"))
      .documents[JsonDocument]()
      .filter(_.content().getString("name").startsWith("a"))
      .collect()

    allDocsStartingWithNameA.foreach(println)
  }

}
