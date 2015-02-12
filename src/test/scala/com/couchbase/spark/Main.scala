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



    val allDocsStartingWithNameA = sc
      .couchbaseView(ViewQuery.from("beer", "brewery_beers"))
      .documents[JsonDocument]()
      .filter(_.content().getString("name").startsWith("a"))
      .collect()

    allDocsStartingWithNameA.foreach(println)
  }

}
