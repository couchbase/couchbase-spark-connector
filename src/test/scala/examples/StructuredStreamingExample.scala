/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package examples

import com.couchbase.spark.kv.KeyValueOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object StructuredStreamingExample {
  def main(args: Array[String]): Unit = {
    val spark = `SparkSession`
      .builder()
      .master("local[*]")
      .appName("Structured Streaming Example")
      .config("spark.couchbase.connectionString", "127.0.0.1")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.implicitBucket", "travel-sample")
      .getOrCreate()

    val sourceDf = spark.readStream
      .format("couchbase.kv")
      .option(KeyValueOptions.StreamFrom, KeyValueOptions.StreamFromBeginning)
      .option(KeyValueOptions.Scope, "inventory")
      .option(KeyValueOptions.Collection, "route")
      .load

    val aggDf = sourceDf.groupBy("collection").count()

    val query = aggDf.writeStream
      .format("console")
      .outputMode(OutputMode.Complete)
      .trigger(Trigger.Once())
      .queryName("kv2console")
      .start

    query.awaitTermination()
  }
}
