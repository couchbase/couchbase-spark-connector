package com.couchbase.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{Descending, SortOrder}
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


    /*val df = sql.n1ql(StructType(
      StructField("name", StringType) ::
      StructField("abv", DoubleType) ::
      StructField("type", StringType) :: Nil
    ))*/

    val df = sql.n1ql()

    df.printSchema()
    df
      .select("name", "abv", "type")
      .where(df("type").equalTo("beer").and(df("abv").lt(20)))
      .sort(df("abv").desc)
      .limit(10)
      .show()

  }

}
