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
package com.couchbase.spark.sql

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CouchbaseDataFrameSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local[2]"
  private val appName = "cb-int-specs1"

  private var spark: SparkSession = null


  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("com.couchbase.username", "Administrator")
      .set("com.couchbase.password", "password")
      .set("com.couchbase.bucket.default", "")
      .set("com.couchbase.bucket.travel-sample", "")
    spark = SparkSession.builder().config(conf).getOrCreate()

    loadData()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  def loadData(): Unit = {

  }

  "The DataFrame API" should "infer the schemas" in {
    val ssc = spark.sqlContext
    import com.couchbase.spark.sql._

    val airline = ssc.read.couchbase(EqualTo("type", "airline"), Map("bucket" -> "travel-sample"))
    val airport = ssc.read.couchbase(EqualTo("type", "airport"), Map("bucket" -> "travel-sample"))
    val route = ssc.read.couchbase(EqualTo("type", "route"), Map("bucket" -> "travel-sample"))
    val landmark = ssc.read.couchbase(EqualTo("type", "landmark"), Map("bucket" -> "travel-sample"))


    airline
      .limit(10)
      .write
      .mode(SaveMode.Overwrite)
      .couchbase(Map("bucket" -> "default"))

    // TODO: validate schemas which are inferred on a field and type basis

  }

  it should "write and ignore" in {
    val ssc = spark.sqlContext
    import com.couchbase.spark.sql._

    // create df, write it twice
    val data = ("Michael", 28, true)
    val df = ssc.createDataFrame(spark.sparkContext.parallelize(Seq(data)))

    df.write
      .mode(SaveMode.Ignore)
      .couchbase(options = Map("idField" -> "_1", "bucket" -> "default"))

    df.write
      .mode(SaveMode.Ignore)
      .couchbase(options = Map("idField" -> "_1", "bucket" -> "default"))
  }

  it should "filter based on a function" in {
    val ssc = spark.sqlContext
    import com.couchbase.spark.sql._

    val airlineBySubstrCountry: DataFrame = ssc.read.couchbase(
      EqualTo("'substr(country, 0, 6)'", "United"), Map("bucket" -> "travel-sample"))

    airlineBySubstrCountry.count() should equal(6797)
  }

}
