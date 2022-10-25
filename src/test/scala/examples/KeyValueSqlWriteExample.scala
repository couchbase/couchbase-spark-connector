/*
 * Copyright (c) 2021 Couchbase, Inc.
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
import com.couchbase.spark.query.QueryOptions
import org.apache.spark.sql.{SaveMode, SparkSession}

object KeyValueSqlWriteExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL Write KV example")
      .config("spark.couchbase.connectionString", "127.0.0.1")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.implicitBucket", "travel-sample")
      .getOrCreate()

    val airlines = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airline'")
      .load()
      .limit(5)

    airlines.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, "travel-sample")
      .option(KeyValueOptions.Durability, KeyValueOptions.MajorityDurability)
      .option(KeyValueOptions.StreamFrom, KeyValueOptions.StreamFromBeginning)
      // .option(KeyValueOptions.Timeout, "10s")
      .mode(SaveMode.Ignore)
      .save()
  }
}
