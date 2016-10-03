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

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder
          .master("local[*]")
        .appName("StructuredWordCount")
        .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 5050)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .option("checkpointLocation", "foo")
      .option("idField", "value")

      .format("com.couchbase.spark.sql")
      .start()

    query.awaitTermination()
  }

}
