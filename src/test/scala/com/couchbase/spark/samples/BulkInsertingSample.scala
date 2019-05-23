/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import com.couchbase.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import java.util
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.stream.Collectors
import org.apache.spark.rdd.RDD
import kantan.csv._
import kantan.csv.ops._

object BulkInsertingSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("BulkInsertingSample")
      .set("com.couchbase.username", "Administrator")
      .set("com.couchbase.password", "password")
      .set("com.couchbase.bucket.default", "")
      // Maximum number of times to retry on error.  Here we're optimising for resilience
      // over speed.
      .set("com.couchbase.maxRetries", "10")
      // The delay will increase exponentially from the minimum to the maximum, in milliseconds.
      .set("com.couchbase.maxRetryDelay", "5000")
      .set("com.couchbase.minRetryDelay", "1000")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    // This movies sampleset can be obtained using:
    // cd ~
    // wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
    // unzip -j "ml-latest.zip"
    val lines = sc.textFile(System.getProperty("user.home") + "/ml-latest/movies.csv")
      .filter((line) => !line.contains("movieId")) // skip first row

    val rdd: RDD[JsonDocument] = lines.map((line) => {
      val reader = line.asUnsafeCsvReader[List[String]](rfc)

      val movie = JsonObject.empty
      val splits = reader.toSeq.head

      // enrichment
      movie.put("type", "movie")

      // transformation
      var originalTitle = splits(1).replace("\"", "")

      // if title contains year
      val m = Pattern.compile("\\(([1-2][0-9][0-9][0-9])\\)").matcher(splits(1))
      if (m.find) {
        movie.put("year", m.group(1).toInt)
        originalTitle = originalTitle.replace(m.group(0), "").trim
      }
      movie.put("title", originalTitle)
      // genres to array
      val arr = JsonArray.from(util.Arrays.stream(splits(2).split("\\|")).collect(Collectors.toList()))
      movie.put("genres", arr)
      // key movie_movieId
      JsonDocument.create("movie_" + splits(0), movie)
    })

    // Write the documents in concurrent batches of 10, per active executor
    rdd.saveToCouchbase(maxConcurrent = 10)
  }
}
