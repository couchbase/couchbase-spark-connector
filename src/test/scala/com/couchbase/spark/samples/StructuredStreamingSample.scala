/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.spark.samples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * This is a simple example that shows how to use structed streaming to
  * write into a couchbase bucket.
  *
  * Note that the idField needs to be set properly since that maps to the
  * document. Use with `nc -lk 5050`
  * in another terminal and then run this program. Write into nc "hello world"
  * enter, and then "hello couchbase" and
  * then observe the bucket documents how their count increases as an
  * aggregation.
  */
object StructuredStreamingSample {

  // Feel Free to add more types here for beers or breweries!
  val schema = StructType(
    StructField("META_ID", StringType) ::
    StructField("type", StringType) ::
    StructField("name", StringType) ::
    StructField("website", StringType) ::
    StructField("abv", DoubleType) ::
    StructField("address", ArrayType(StringType)) :: Nil
  )


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .config("com.couchbase.bucket.beer-sample", "")
      .getOrCreate()

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("com.couchbase.spark.sql")
      .schema(schema)
      .load()

    // You can display all data or a grouping, as an example.
    val count = false

    val query = if (count) {
      // Generate running word count
      val typeCounts = lines.groupBy("type").count()

      // Start running the query that prints the running counts to the console
      typeCounts.writeStream
        .outputMode("complete")
        .format("console")
        .start()
    } else {
      // Generate running word count

      // Start running the query that prints the running counts to the console
       lines.writeStream
        .outputMode("append")
        .format("console")
        .start()
    }

    query.awaitTermination()
  }

}
