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
import com.couchbase.client.java.analytics.AnalyticsQuery
import org.apache.spark.sql.SparkSession
import com.couchbase.spark._


object Examples {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("CouchbaseSparkExamples")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to Couchbase Server on localhost
      .config("spark.couchbase.username", "Administrator") // with given credentials
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket
      .getOrCreate()

    // The SparkContext for easy access
    val sc = spark.sparkContext

    // #tag::analytics[]
    sc.couchbaseAnalytics(AnalyticsQuery.simple("""SELECT "Hello, world!" AS greeting"""))
      .collect()
      .foreach(println(_))
    // #end::analytics[]

  }
}