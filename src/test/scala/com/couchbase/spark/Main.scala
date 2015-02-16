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
      .set("com.couchbase.bucket.beer-sample", "")
      //.set("com.couchbase.bucket.default", "")
      //.setMaster("spark://daschlbook.local:7077")
      .setMaster("local[*]")
      .setAppName("test")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val beers = sqlContext.jsonRDD(sc
      .couchbaseView(query = ViewQuery.from("all", "all"))
      .map(row => row.id)
      .couchbaseGet[RawJsonDocument]()
      .map(doc => doc.content()))

    beers.printSchema()

    beers.registerTempTable("beers")

    val strongBeers = sqlContext.sql("SELECT name, abv FROM beers WHERE abv > 5.0 ORDER BY abv DESC LIMIT 10")

    strongBeers.foreach(println)

  }

}
