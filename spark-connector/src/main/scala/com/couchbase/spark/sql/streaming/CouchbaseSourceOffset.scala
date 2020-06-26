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

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

/**
  * An Offset for the [[CouchbaseSource]].
  */
case class CouchbaseSourceOffset(partitionToOffsets: Map[Short, Long]) extends Offset {
  override val json = JsonUtils.partitionOffsets(partitionToOffsets)
}

/**
  * Companion object of the [[CouchbaseSourceOffset]].
  */
object CouchbaseSourceOffset {

  def getPartitionOffset(offset: Offset): Map[Short, Long] = {
    offset match {
      case o: CouchbaseSourceOffset => o.partitionToOffsets
      case so: SerializedOffset => CouchbaseSourceOffset(so).partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to CouchbaseSourceOffset")
    }
  }

  def convertToCouchbaseSourceOffset(offset: Offset): CouchbaseSourceOffset = {
    offset match {
      case o: CouchbaseSourceOffset => o
      case so: SerializedOffset => CouchbaseSourceOffset(so)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to CouchbaseSourceOffset")
    }
  }

  /**
    * Returns [[CouchbaseSourceOffset]] from a JSON SerializedOffset.
    */
  def apply(offset: SerializedOffset): CouchbaseSourceOffset = {
    CouchbaseSourceOffset(JsonUtils.partitionOffsets(offset.json))
  }

}