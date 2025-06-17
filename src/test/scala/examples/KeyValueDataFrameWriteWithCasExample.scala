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

import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.kv.KeyValueOptions
import com.couchbase.spark.query.QueryOptions
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object KeyValueDataFrameWriteWithCasExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CAS Replace With CAS Example")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.connectionString", "localhost")
      .config("spark.couchbase.implicitBucket", "travel-sample")
      .getOrCreate()

    import spark.implicits._

    println("=== CAS Replace With CAS Mode Example ===")

    // Step 1: Insert some initial documents
    println("Step 1: Inserting initial documents...")
    val initialData = Seq(
      ("cas_doc1", "initial_value1"),
      ("cas_doc2", "initial_value2"),
      ("cas_doc3", "initial_value3")
    ).toDF("__META_ID", "content")

    initialData.write
      .format("couchbase.kv")
      .mode(SaveMode.Overwrite)
      .save()

    println("Initial documents inserted successfully")

    // Step 2: Read documents with CAS values included
    println("\nStep 2: Reading documents with CAS values...")
    val docsWithCas = spark.read
      .format("couchbase.query")
      // Will include a column named DefaultConstants.DefaultCasFieldName with the CAS results
      .option(QueryOptions.OutputCas, "true")
      .load()

    println("Documents with CAS:")
    docsWithCas.show()

    docsWithCas.write
      .format("couchbase.kv")
      .option(KeyValueOptions.WriteMode, KeyValueOptions.WriteModeReplace)
      .option(KeyValueOptions.CasFieldName, DefaultConstants.DefaultCasFieldName)
      .save()

    println("Documents updated successfully with CAS")

    println("\n=== Example Complete ===")
    spark.stop()
  }
}