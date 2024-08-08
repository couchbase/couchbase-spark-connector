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

import com.couchbase.spark.analytics.AnalyticsOptions
import com.couchbase.spark.columnar.ColumnarOptions
import org.apache.spark.sql.SparkSession

object ColumnarSqlExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL Columnar example")
      .config("spark.couchbase.connectionString", "COLUMNAR_ENDPOINT")
      .config("spark.couchbase.username", "test")
      .config("spark.couchbase.password", "Password!1")
      .config("spark.ssl.insecure", "true")
      .getOrCreate()

    val airlines = spark.read
      .format("couchbase.columnar")
      .option(ColumnarOptions.Database, "travel-sample")
      .option(ColumnarOptions.Scope, "inventory")
      .option(ColumnarOptions.Collection, "airline")
      .load()

    airlines.show()
  }
}
