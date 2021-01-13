/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.client.dcp.Client
import com.couchbase.client.dcp.highlevel.{DatabaseChangeListener, Deletion, FlowControlMode, Mutation, StreamFailure}
import com.couchbase.spark.sql.QuerySource
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}

class CouchbaseSource(sqlContext: SQLContext, userSchema: Option[StructType],
                      parameters: Map[String, String])
  extends SupportsAdmissionControl
    with Source
    with Logging {

  private var dcpClient: Client = null

  init()

  private def init() {
    dcpClient = Client
      .builder()
      .credentials("Administrator", "password")
      .hostnames("localhost")
      .bucket("travel-sample")
      .flowControl(64 * 1024 * 1024)
      .build()

    dcpClient.listener(new DatabaseChangeListener {
      override def onFailure(streamFailure: StreamFailure): Unit = {

      }

      override def onMutation(mutation: Mutation): Unit = {

      }

      override def onDeletion(deletion: Deletion): Unit = {

      }
    }, FlowControlMode.AUTOMATIC)

    dcpClient.connect().block()
  }

  override def schema: StructType = userSchema.getOrElse(CouchbaseSource.DEFAULT_SCHEMA)

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    throw new UnsupportedOperationException
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    throw new UnsupportedOperationException
  }

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def stop(): Unit = {
    dcpClient.disconnect().block()
  }
}

object CouchbaseSource {
  val DEFAULT_SCHEMA: StructType = StructType(Seq(
    StructField(QuerySource.DEFAULT_ID_FIELD_NAME, StringType),
    StructField("value", BinaryType)
  ))
}