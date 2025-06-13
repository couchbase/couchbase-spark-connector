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
package com.couchbase.spark.kv

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import reactor.core.scala.publisher.SMono

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt

class KeyValueErrorDocumentHandler(
    writeConfig: KeyValueWriteConfig,
    couchbaseConfig: CouchbaseConfig
) extends Logging
    with Serializable {

  private lazy val errorCollection = {
    val errorsBucket = writeConfig.errorBucket.getOrElse(
      throw new IllegalArgumentException(
        "errorBucket must be specified when using ErrorDocumentHandler"
      )
    )
    val errorsScope = writeConfig.errorScope.getOrElse(DefaultConstants.DefaultScopeName)
    val errorsCollection =
      writeConfig.errorCollection.getOrElse(DefaultConstants.DefaultCollectionName)

    CouchbaseConnection(writeConfig.connectionIdentifier)
      .cluster(couchbaseConfig)
      .bucket(errorsBucket)
      .scope(errorsScope)
      .collection(errorsCollection)
  }

  def handleError(
      documentId: String,
      originalBucket: String,
      originalScope: String,
      originalCollection: String,
      error: Throwable
  ): Unit = {
    val errorDocId =
      s"error_${originalBucket}.${originalScope}.${originalCollection}_${documentId}_${System.currentTimeMillis()}"

    val errorJson = JsonObject.create
      .put("class", error.getClass.getName)
      .put("simpleClass", error.getClass.getSimpleName)

    error match {
      case e: CouchbaseException => errorJson.put("context", e.context.exportAsMap)
      // Logging this for CouchbaseException would include the context again
      case _ => errorJson.put("message", error.getMessage)
    }

    val errorDocJson = JsonObject.create
      .put("timestamp", java.time.Instant.now().toString)
      .put("documentId", documentId)
      .put("bucket", originalBucket)
      .put("scope", originalScope)
      .put("collection", originalCollection)
      .put("error", errorJson)

    try {
      errorCollection.upsert(errorDocId, errorDocJson)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to write error document for $documentId: ${e.getMessage}")
    }
  }
}
