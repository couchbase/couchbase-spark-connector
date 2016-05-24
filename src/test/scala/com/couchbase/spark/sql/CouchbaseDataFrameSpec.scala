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

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class CouchbaseDataFrameSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val master = "local[2]"
  private val appName = "cb-int-specs1"
  private val bucketName = "travel-sample"

  private var sparkContext: SparkContext = null

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("com.couchbase.bucket.default", "")
      .set("com.couchbase.bucket.travel-sample", "")
    sparkContext = new SparkContext(conf)

    loadData()
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  def loadData(): Unit = {

  }

  "The DataFrame API" should "infer the schemas" in {
    val ssc = new SQLContext(sparkContext)
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

  it should "query with predicate" in {

  }

}
