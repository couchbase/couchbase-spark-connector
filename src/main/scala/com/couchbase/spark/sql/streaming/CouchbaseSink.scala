/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.spark.sql.streaming

import com.couchbase.spark.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types.StringType
import com.couchbase.spark.sql._
import com.couchbase.spark._
import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import scala.concurrent.duration._


/**
  * A simple Structured Streaming sink which writes the data frame to the bucket.
  *
  * Note that for now Upsert is always used internally to not run into document already
  * exists exception, especially when writing aggregates.
  *
  * @param options options passed from the upper level down to the dataframe writer.
  */
class CouchbaseSink(options: Map[String, String]) extends Sink with Logging {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val bucketName = options.get("bucket").orNull
    val idFieldName = options.getOrElse("idField", DefaultSource.DEFAULT_DOCUMENT_ID_FIELD)
    val removeIdField = options.getOrElse("removeIdField", "true").toBoolean
    val timeout = options.get("timeout").map(v => Duration(v.toLong, MILLISECONDS))

    data.toJSON
      .queryExecution
      .toRdd
      .map(_.get(0, StringType).asInstanceOf[UTF8String].toString())
      .map { rawJson =>
          val encoded = JsonObject.fromJson(rawJson)
          val id = encoded.get(idFieldName)

          if (id == null) {
              throw new Exception(s"Could not find ID field $idFieldName in $encoded")
          }

          if (removeIdField) {
              encoded.removeKey(idFieldName)
          }

          JsonDocument.create(id.toString, encoded)
      }
      .saveToCouchbase(bucketName, StoreMode.UPSERT, timeout)
  }

}
