/*
 * Copyright (c) 2025 Couchbase, Inc.
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
import examples.util.CollectionUtils
import org.apache.spark.sql.{SaveMode, SparkSession}


object ErrorBucketExample {
  def main(args: Array[String]): Unit = {
    val bucketName = "travel-sample"
    val errorScopeName = "write-errors"  
    val errorCollectionName = "kv"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark ErrorBucket example")
      .config("spark.couchbase.connectionString", "localhost")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .getOrCreate()

    // Create the error collection if it doesn't exist
    CollectionUtils.ensureCollectionExists(spark, bucketName, errorScopeName, errorCollectionName)

    val airlines = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airline'")
      .option(KeyValueOptions.Bucket, bucketName)
      .load()
      .limit(5)

    println("Writing airlines data with error handling...")
    airlines.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, bucketName)
      // Failed operations will create error documents in the specified collection
      .option(KeyValueOptions.ErrorBucket, bucketName)
      .option(KeyValueOptions.ErrorScope, errorScopeName)
      .option(KeyValueOptions.ErrorCollection, errorCollectionName)
      .mode(SaveMode.Ignore)
      .save()

    // Read back any error documents that were created
    println("Checking for error documents...")
    val errorDocs = spark.read
      .format("couchbase.query")
      .option(KeyValueOptions.Bucket, bucketName)
      .option(KeyValueOptions.Scope, errorScopeName)
      .option(KeyValueOptions.Collection, errorCollectionName)
      .load()

    val errorCount = errorDocs.count()
    if (errorCount > 0) {
      println(s"Found $errorCount error documents:")
      errorDocs.show(truncate = false)
      
      println("Error document summary:")
      errorDocs.select("timestamp", "documentId", "error.simpleClass")
        .show(truncate = false)
    } else {
      println("No error documents found - all operations succeeded!")
    }

    spark.stop()
  }
}

object ErrorHandlerExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL analytics example")
      .config("spark.couchbase.connectionString", "localhost")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .getOrCreate()

    val airlines = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Filter, "type = 'airline'")
      .option(KeyValueOptions.Bucket, "travel-sample")
      .load()
      .limit(5)

    airlines.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, "travel-sample")
      // See this class for an explanation
      .option(KeyValueOptions.ErrorHandler, "com.couchbase.spark.kv.ErrorHandler")
      .mode(SaveMode.Ignore)
      .save()

    spark.stop()
  }
}
