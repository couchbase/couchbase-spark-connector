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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext}
import com.couchbase.spark.sql._
import org.apache.spark.sql.sources.EqualTo

// Airline has subset of the fields that are in the database
case class Airline(name: String, iata: String, icao: String, country: String)

object DatasetSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DatasetSample")
      .set("com.couchbase.bucket.travel-sample", "")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._

    val airlines = sql.read.couchbase(schemaFilter = EqualTo("type", "airline")).as[Airline]

    // Print schema
    airlines.printSchema()

    // Print airlines that start with A
    println(airlines.map(_.name).filter(_.toLowerCase.startsWith("a")).foreach(println(_)))

  }

}
