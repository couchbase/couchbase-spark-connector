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

import com.couchbase.spark._
// #tag::imports[]
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.view.ViewQuery
import org.apache.spark.sql.SparkSession
// #end::imports[]


object Examples {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("WorkingWithRDDs")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to Couchbase Server on localhost
      .config("spark.couchbase.username", "Administrator") // with given credentials
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.bucket.travel-sample", "") // open the travel-sample bucket
      .getOrCreate()

    // The SparkContext for easy access
    val sc = spark.sparkContext

    // #tag::create-rdd[]
    sc.couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748"))
      .collect()
      .foreach(println)
    // #end::create-rdd[]

    // #tag::view[]
    sc.couchbaseView(ViewQuery.from("airlines", "by_name").limit(10))
      .collect()
      .foreach(println)
    // #end::view[]

    // #tag::view-get[]
    sc.couchbaseView(ViewQuery.from("airlines", "by_name").limit(10))
      .map(_.id)
      .couchbaseGet[JsonDocument]()
      .collect()
      .foreach(println)
    // #end::view-get[]

    def query1() {
      // #tag::query[]
      val query = "SELECT name FROM `travel-sample` WHERE type = 'airline' ORDER BY name ASC LIMIT 10"
      sc.couchbaseQuery(N1qlQuery.simple(query))
        .collect()
        .foreach(println)
      // #end::query[]
    }

    def query2() {
      // #tag::query-get[]
      val query = "SELECT META(`travel-sample`).id as id FROM `travel-sample` WHERE type = 'airline' ORDER BY name ASC LIMIT 10"

      sc.couchbaseQuery(N1qlQuery.simple(query))
        .map(_.value.getString("id"))
        .couchbaseGet[JsonDocument]()
        .collect()
        .foreach(println)
      // #end::query-get[]
    }

    def query3() {
      // #tag::query3[]
      val query = "SELECT name, country FROM `travel-sample` WHERE type = 'airline' ORDER BY name"
      sc
        .couchbaseQuery(N1qlQuery.simple(query))
        .groupBy(_.value.getString("country"))
        .map(pair => {
          val airports = JsonArray.create()
          val content = JsonObject.create().put("airports", airports)
          pair._2.map(_.value.getString("name")).foreach(airports.add)
          JsonDocument.create("airports::" + pair._1, content)
        })
        .saveToCouchbase()
      // #end::query3[]
    }

    def subdoc() {
      // #tag::subdoc[]
      sc.parallelize(Seq("airline_10123"))
        .couchbaseSubdocLookup(get = Seq("name", "iata"), exists = Seq("foobar"))
        .collect()
        .foreach(println)
      // #end::subdoc[]
    }
  }
}