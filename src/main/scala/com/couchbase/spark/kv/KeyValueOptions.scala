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
package com.couchbase.spark.kv

import com.couchbase.spark.DefaultConstants

/**
 * Helper object to provide type-safe keys and values for Spark SQL query options.
 */
object KeyValueOptions {

  /**
   * Option Key: The name of the bucket, which overrides the implicit bucket configured.
   */
  val Bucket = "bucket"

  /**
   * Option Key: The name of the scope, which overrides the implicit scope (if configured at all).
   *
   * Note: only works against Couchbase Server 7.0 or later.
   */
  val Scope = "scope"

  /**
   * Option Key: The name of the collection, which overrides the implicit collection (if configured at all).
   *
   * Note: only works against Couchbase Server 7.0 or later.
   */
  val Collection = "collection"

  /**
   * Option Key: The name of the collections (comma separated) that will be used for Spark Streaming.
   *
   * The [[Collection]] arg can also be used if only one needs to be specified.
   */
  val Collections = "collections"

  /**
   * Option Key: The field name of the document ID, used to override the default.
   *
   * The default can be located in [[DefaultConstants.DefaultIdFieldName]]
   */
  val IdFieldName = "idFieldName"

  /**
   * Option key: The durability level of write operations, used to override the default.
   *
   * The default is "none", so no durability applied on write operations.
   */
  val Durability = "durability"

  /**
   * Option Key: The timeout to use which overrides the default configured.
   *
   * The value is a string and must be parsable from a scala Duration.
   */
  val Timeout = "timeout"

  /**
   * Option Key: The number of partitions to use when using Spark Streaming.
   *
   * If not provided, defaults to the defaultParallelism of the spark context.
   */
  val NumPartitions = "numPartitions"

  /**
   * Option Key: Controls from where the connector starts streaming from.
   *
   * If not provided, defaults to
   */
  val StreamFrom = "streamFrom"

  /**
   * Option Key: The type of streaming metadata that should be sent to a downstream consumer per row.
   *
   * Choose either [[StreamMetaDataNone]], [[StreamMetaDataBasic]] or [[StreamMetaDataFull]] as the values, where
   * Basic is the default value if none provided.
   */
  val StreamMetaData = "streamMetaData"

  /**
   * Option Key: If the content of a document should also be streamed, or just the id (Spark Streaming only).
   *
   * The value is a "true" or "false", where "true" is the default.
   */
  val StreamContent = "streamContent"

  /**
   * Option Key: The flow control buffer size to use for a DCP stream (spark streaming only).
   *
   * The value is a buffer size in bytes and defaults to 10MB (1024 * 1024 * 10).
   */
  val StreamFlowControlBufferSize = "streamFlowControlBufferSize"

  /**
   * Option Key: the polling interval to mitigate rollbacks.
   *
   * The value is a string and must be parsable from a scala Duration (defaults to 100ms). If
   * set to 0, the polling interval is disabled.
   */
  val StreamPersistencePollingInterval = "streamPersistencePollingInterval"

  /**
   * Option value: Majority Durability - to be used with [[Durability]] as the key.
   */
  val MajorityDurability = "majority"

  /**
   * Option value: Majority And Persist To Active Durability - to be used with [[Durability]] as the key.
   */
  val MajorityAndPersistToActiveDurability = "majorityAndPersistToActive"

  /**
   * Option value: Persist To Majority Durability - to be used with [[Durability]] as the key.
   */
  val PersistToMajorityDurability = "PersistToMajority"

  /**
   * Option value: Do not stream any additional metadata for a spark stream.
   */
  val StreamMetaDataNone = "none"

  /**
   * Option value: Only stream basic metadata information.
   */
  val StreamMetaDataBasic = "basic"

  /**
   * Option value: Stream all mutation metadata that is available.
   */
  val StreamMetaDataFull = "full"

  /**
   * Option value: If an offset has been saved start from there, otherwise start the Structured stream from "now"
   * (no mutations are streamed from before this point).
   */
  val StreamFromNow = "fromNow"

  /**
   * Option value: If an offset has been saved start from there, otherwise start the Structured stream from
   * "beginning" (previous mutations are streamed from before this point).
   */
  val StreamFromBeginning = "fromBeginning"

}
