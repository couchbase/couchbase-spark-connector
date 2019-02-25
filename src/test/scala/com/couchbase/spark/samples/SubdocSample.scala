/*
 * Copyright (c) 2015 Couchbase, Inc.
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
import org.apache.spark.{SparkConf, SparkContext}
import com.couchbase.spark._

object SubdocSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DatasetSample")
      .set("com.couchbase.username", "Administrator")
      .set("com.couchbase.password", "password")
      .set("com.couchbase.bucket.travel-sample", "")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val result = sc
      .parallelize(Seq("airline_10123"))
      .couchbaseSubdocLookup(get = Seq("name", "iata"), exists = Seq("foobar"))
      .collect()

    val r2  = sc.couchbaseSubdocLookup(Seq("airline_10123"), Seq("name", "iata"))

    // Prints
    // SubdocLookupResult(
    //    airline_10123,0,Map(name -> Texas Wings, iata -> TQ),Map(foobar -> false)
    // )
    result.foreach(println)

    // Prints
    // SubdocLookupResult(
    //    airline_10123,0,Map(name -> Texas Wings, iata -> TQ),Map()
    // )
    r2.foreach(println)

  }
}
