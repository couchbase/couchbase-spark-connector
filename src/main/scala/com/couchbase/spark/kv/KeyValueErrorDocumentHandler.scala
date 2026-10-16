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
import java.security.MessageDigest
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
    val errorDocId = KeyValueErrorDocumentHandler.errorDocumentId(
      originalBucket,
      originalScope,
      originalCollection,
      documentId,
      System.currentTimeMillis()
    )

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
      errorCollection.upsert(errorDocId, errorDocJson).get
    } catch {
      case e: Exception =>
        logWarning(s"Failed to write error document for $documentId: ${e.getMessage}")
    }
  }
}

object KeyValueErrorDocumentHandler {

  /** SPARKC-243: The ID of the error document for one failed operation.
    *
    * The original document's location and ID are hashed, rather than included verbatim, so the ID is always
    * well within the 250-byte key limit whatever the length of the original ID.  All of them are in the error
    * document's body.  All failures of one document share the same hash, and the timestamp separates them.
    *
    * '/' cannot appear in bucket, scope or collection names, so it separates them unambiguously.
    */
  private[kv] def errorDocumentId(
      bucket: String,
      scope: String,
      collection: String,
      documentId: String,
      timestampMillis: Long
  ): String = {
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(s"$bucket/$scope/$collection/$documentId".getBytes(StandardCharsets.UTF_8))
    // 128 bits is plenty to avoid collisions.
    val hash = digest.take(16).map(b => f"${b & 0xff}%02x").mkString
    s"error_${hash}_$timestampMillis"
  }
}
