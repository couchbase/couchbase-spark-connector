/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark.sql

import org.apache.spark.sql.SQLContext
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
      .set("com.couchbase.bucket.travel-sample", "")
    sparkContext = new SparkContext(conf)

    loadData()
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  def loadData(): Unit = {

  }

  "The Couchbase DF API" should "infer the schema from documents" in {
    val sqlContext = new SQLContext(sparkContext)
    import com.couchbase.spark.sql._

    val df = sqlContext.read.couchbase(EqualTo("type", "airline"))

    df.printSchema()

    df.filter(df("callsign").isNotNull).orderBy(df("icao").asc).show(10)

  }

  it should "query with predicate" in {

  }

}
