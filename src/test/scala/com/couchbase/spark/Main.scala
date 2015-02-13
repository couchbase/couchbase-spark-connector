package com.couchbase.spark

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.view.ViewQuery
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("couchbase.bucket", "beer-sample")
      .setMaster("local")
      .setAppName("test")

    val sc = new SparkContext(conf)

    // Use ids in the codes
    val docs = sc.couchbaseGet(Seq("id"))
    val docs1 = sc.couchbaseGet(Seq("id1", "id2"), 10)

    // or read ids from files
    val docs4 = sc.textFile("this_is_a_path").couchbaseGet

    val allDocsStartingWithNameA = sc
      .couchbaseView(ViewQuery.from("beer", "brewery_beers"))
      .map(row => row.id)
      .couchbaseGet[JsonDocument]
      .filter(_.content().getString("name").startsWith("a"))
      .collect()

    allDocsStartingWithNameA.foreach(println)

    val rdd: RDD[(String, JsonObject)] = null // Just test if it can compile
    rdd.documentRDD.saveToCouchbase()

    val rdd2: RDD[JsonDocument] = null // Just test if it can compile
    rdd2.saveToCouchbase()
  }

}
