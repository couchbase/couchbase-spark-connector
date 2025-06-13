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
package examples.util

import com.couchbase.client.scala.manager.collection.{CollectionSpec, CreateCollectionSettings}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/** Utility class for managing Couchbase collections in examples.
  */
object CollectionUtils {

  def ensureCollectionExists(
      spark: SparkSession,
      bucketName: String,
      scopeName: String,
      collectionName: String
  ): Unit = {
    val config  = CouchbaseConfig(spark.sparkContext.getConf)
    val sdk     = CouchbaseConnection()
    val cluster = sdk.cluster(config)
    val bucket  = cluster.bucket(bucketName)

    try {
      val bucketManager = bucket.collections

      // Create scope if it doesn't exist
      bucketManager.createScope(scopeName) match {
        case Success(_) => println(s"Created scope: $scopeName")
        case Failure(ex) if ex.getMessage.contains("already exists") =>
          println(s"Scope $scopeName already exists")
        case Failure(ex) =>
          println(s"Failed to create scope: ${ex.getMessage}")
      }

      // Create collection if it doesn't exist
      bucketManager.createCollection(scopeName, collectionName, CreateCollectionSettings()) match {
        case Success(_) => println(s"Created collection: $scopeName.$collectionName")
        case Failure(ex) if ex.getMessage.contains("already exists") =>
          println(s"Collection $scopeName.$collectionName already exists")
        case Failure(ex) =>
          println(s"Failed to create collection: ${ex.getMessage}")
      }
    } catch {
      case ex: Exception => println(s"Error during collection setup: ${ex.getMessage}")
    }
  }
}
