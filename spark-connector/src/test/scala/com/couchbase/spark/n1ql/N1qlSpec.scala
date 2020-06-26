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
package com.couchbase.spark.n1ql

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.error.QueryExecutionException
import com.couchbase.client.java.query.N1qlQuery
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest._
import com.couchbase.spark._
import com.couchbase.spark.connection.CouchbaseConnection
import com.couchbase.spark.sql.N1QLRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.control.NonFatal

class N1qlSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private val master = "local[2]"
  private val appName = "cb-int-specs1"

  private var spark: SparkSession = _


  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      // Open 2 buckets as tests below rely on it
      .config("com.couchbase.bucket.default", "")
      .config("com.couchbase.bucket.travel-sample", "")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    CouchbaseConnection().stop()
    spark.stop()
  }

  test("Creating N1QLRelation with default bucket, when two buckets exist, should fail") {
    assertThrows[IllegalStateException] {
      spark.read
        .format("com.couchbase.spark.sql.DefaultSource")
        .option("schemaFilter", N1QLRelation.filterToExpression(EqualTo("type", "airline")))
        .option("schemaFilter", "`type` = 'airline'")
        .schema(StructType(StructField("name", StringType) :: Nil))
        .load()
    }
  }

  test("Creating N1QLRelation with non-default bucket, when two buckets exist, should succeed") {
    spark.read
      .format("com.couchbase.spark.sql.DefaultSource")
      .option("schemaFilter", N1QLRelation.filterToExpression(EqualTo("type", "airline")))
      .option("schemaFilter", "`type` = 'airline'")
      .option("bucket", "travel-sample")
      .schema(StructType(StructField("name", StringType) :: Nil))
      .load()
  }

  test("N1QL failures should fail the Observable") {
    try {
      spark.sparkContext
        .couchbaseQuery(N1qlQuery.simple("BAD QUERY"), bucketName = "default")
        .collect()
        .foreach(println)
      fail()
    }
    catch {
      case e: SparkException =>
        assert (e.getCause.isInstanceOf[QueryExecutionException])
        val err = e.getCause.asInstanceOf[QueryExecutionException]
        assert (err.getMessage == "syntax error - at QUERY")
      case NonFatal(e) =>
        println(e)
        fail()
    }
  }
}
