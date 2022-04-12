/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.spark.kv.StreamFromVariants.StreamFrom

case class KeyValueStreamConfig(
  streamFrom: StreamFrom,
  numInputPartitions: Int,
  bucket: String,
  scope: Option[String],
  collections: Seq[String],
  streamContent: Boolean,
  streamXattrs: Boolean,
  flowControlBufferSize: Option[Int],
  persistencePollingInterval: Option[String]
)

object StreamFromVariants extends Enumeration {
  type StreamFrom = Value

  val FromNow, FromBeginning, FromOffsetOrNow, FromOffsetOrBeginning = Value

  implicit class StreamFromVariantsValue(from: StreamFrom) {
    def fromBeginning(): Boolean = {
      from match {
        case FromBeginning | FromOffsetOrBeginning => true
        case _ => false
      }
    }
    def asDcpStreamFrom: com.couchbase.client.dcp.StreamFrom = {
      if (fromBeginning()) {
        com.couchbase.client.dcp.StreamFrom.BEGINNING
      } else {
        com.couchbase.client.dcp.StreamFrom.NOW
      }
    }
  }
}