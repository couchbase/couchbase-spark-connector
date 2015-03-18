package com.couchbase.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{Descending, SortOrder}
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types._
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

    // manual schema
    /*val df = sql.n1ql(StructType(
      StructField("name", StringType) ::
      StructField("abv", DoubleType) ::
      StructField("type", StringType) :: Nil
    ))*/

    // schema inference
    val df = sql.n1ql(filter = EqualTo("type", "beer"))

    df.printSchema()

    df
      .select("name", "abv", "type")
      .where(df("abv").lt(20.0))
      .sort(df("abv").desc)
      .show(10)

  }

}
