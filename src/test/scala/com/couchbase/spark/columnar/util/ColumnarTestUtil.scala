package com.couchbase.spark.columnar.util

import com.couchbase.spark.columnar.ColumnarOptions
import org.apache.spark.sql.{DataFrameReader, SparkSession}

object ColumnarTestUtil {
  def basicDataFrameReader(spark: SparkSession): DataFrameReader = {
    spark.read
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, "travel-sample")
      .option(ColumnarOptions.Scope, "inventory")
      .option(ColumnarOptions.Collection, "airline")
  }
}
