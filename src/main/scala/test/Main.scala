package test

import com.couchbase.client.java.document.{RawJsonDocument, JsonDocument}
import com.couchbase.spark._
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)

    val docs = sc.couchbaseGet("foo")


    println(docs.collect())

  }

}
