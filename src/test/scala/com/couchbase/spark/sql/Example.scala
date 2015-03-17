package com.couchbase.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{FloatType, StringType}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * .
 *
 * @author Michael Nitschinger
 * @since
 */
object Example {

  def main(args: Array[String]): Unit = {

    System.setProperty("com.couchbase.queryEnabled", "true");

    val conf = new SparkConf().setMaster("local[*]").set("com.couchbase.bucket.beer-sample", "").setAppName("sqltest")
    val sc = new SparkContext(conf)

    val sql = new SQLContext(sc)

    val dataFrame = sql.n1ql(Map(
      "name" -> StringType,
      "abv" -> FloatType,
      "type" -> StringType
    ))

    dataFrame.printSchema()

    dataFrame.filter("type = 'beer'").limit(5).show()

  }

}
