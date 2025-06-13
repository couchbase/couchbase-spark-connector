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

/** Helper object to provide type-safe keys and values for Spark SQL query options.
  */
object KeyValueOptions {

  /** Option Key: Instead of using the "default" connection, allows to connect to a different
    * cluster.
    */
  val ConnectionIdentifier = "connectionIdentifier"

  /** Option Key: The name of the bucket, which overrides the implicit bucket configured.
    */
  val Bucket = "bucket"

  /** Option Key: The name of the scope, which overrides the implicit scope (if configured at all).
    *
    * Note: only works against Couchbase Server 7.0 or later.
    */
  val Scope = "scope"

  /** Option Key: The name of the collection, which overrides the implicit collection (if configured
    * at all).
    *
    * Note: only works against Couchbase Server 7.0 or later.
    */
  val Collection = "collection"

  /** Option Key: The name of the collections (comma separated) that will be used for Spark
    * Streaming.
    *
    * The [[Collection]] arg can also be used if only one needs to be specified.
    */
  val Collections = "collections"

  /** Option Key: The field name of the document ID, used to override the default.
    *
    * The default can be located in [[DefaultConstants.DefaultIdFieldName]]
    */
  val IdFieldName = "idFieldName"

  /** Option key: The durability level of write operations, used to override the default.
    *
    * The default is "none", so no durability applied on write operations.
    */
  val Durability = "durability"

  /** Option Key: The timeout to use which overrides the default configured.
    *
    * The value is a string and must be parsable from a scala Duration.
    */
  val Timeout = "timeout"

  /** Option Key: The number of partitions to use when using Spark Streaming.
    *
    * If not provided, defaults to the defaultParallelism of the spark context.
    */
  val NumPartitions = "numPartitions"

  /** Option Key: Controls from where the connector starts streaming from.
    *
    * If not provided, defaults to
    */
  val StreamFrom = "streamFrom"

  /** Option Key: The type of streaming metadata that should be sent to a downstream consumer per
    * row.
    *
    * Choose either [[StreamMetaDataNone]], [[StreamMetaDataBasic]] or [[StreamMetaDataFull]] as the
    * values, where Basic is the default value if none provided.
    */
  val StreamMetaData = "streamMetaData"

  /** Option Key: If the content of a document should also be streamed, or just the id (Spark
    * Streaming only).
    *
    * The value is a "true" or "false", where "true" is the default.
    */
  val StreamContent = "streamContent"

  /** Option Key: The flow control buffer size to use for a DCP stream (spark streaming only).
    *
    * The value is a buffer size in bytes and defaults to 10MB (1024 * 1024 * 10).
    */
  val StreamFlowControlBufferSize = "streamFlowControlBufferSize"

  /** Option Key: the polling interval to mitigate rollbacks.
    *
    * The value is a string and must be parsable from a scala Duration (defaults to 100ms). If set
    * to 0, the polling interval is disabled.
    */
  val StreamPersistencePollingInterval = "streamPersistencePollingInterval"

  /** Option Key: Enables writing failed documents to a specified Couchbase collection.
   *
   * This handler creates error documents with the pattern:
   * "error_{orig_doc_bucket}.{orig_doc_scope}.{orig_doc_collection}_{orig_doc_id}_{timestamp}"
   *
   * One will be created for each failing underlying KeyValue operation (e.g. an individual upsert,
   * insert or similar).
   *
   * The error document currently contains JSON with the following fields:
   *   - "timestamp": ISO 8601 timestamp when the error occurred
   *   - "documentId": The original document ID that failed
   *   - "bucket": The original bucket name
   *   - "scope": The original scope name
   *   - "collection": The original collection name
   *   - "error": Nested object containing:
   *     - "class": Full class name of the exception
   *     - "simpleClass": Simple class name of the exception
   *     - "context": Couchbase context map (for Couchbase errors)
   *
   * NOTE: these fields should be regarded as somewhat volatile. While the fields above are
   * unlikely to be removed, additional fields may be added in future versions, and not all fields
   * may be available on all every operation. Applications should handle missing fields gracefully.
   * In particular the contents of the "context" field should not be relied upon.
   *
   * The bucket, scope, and collection must already exist, otherwise these error documents will be
   * unable to be written.
   *
   * IMPORTANT: When ErrorBucket is specified, individual write operation failures will no longer
   * cause the entire Spark job to fail.
   *
   * Both ErrorBucket and ErrorHandler may be used together.
   *
   * Note that error processing is handled by a bounded background queue, on the Spark executor.
   * If this queue is exceeded, additional failures will be discarded with a warning logged in the executor logs.
   * This prevents excessive memory usage during pathological error conditions.
   */
  val ErrorBucket = "errorBucket"

  /** Option Key: The scope name where error documents should be stored.
   *
   * The scope must already exist. If not specified, uses default scope.
   */
  val ErrorScope = "errorScope"

  /** Option Key: The collection name where error documents should be stored.
   *
   * The collection must already exist. If not specified, uses default collection.
   */
  val ErrorCollection = "errorCollection"

  /** Option Key: an error handler class name for handling write operation errors.
    *
    * The value should be a fully qualified class name that implements KeyValueWriteErrorHandler.  The class code must be
    * present on the classpath of what the Spark executor is given to run.
    *
    * When ErrorHandler is specified, individual write operation failures will no longer
    * cause the entire Spark job to fail.
    *
    * Note the ErrorHandler will be run on the Spark executor - NOT the main application.  See
    * [[com.couchbase.spark.kv.ErrorHandler]] for why this is important, and for the limitations it
    * creates.  Users should prefer ErrorBucket for error handling.
    *
    * Note that error processing is handled by a bounded background queue, on the Spark executor.
    * If this queue is exceeded, additional failures will be discarded with a warning logged in the executor logs.
    * This prevents excessive memory usage during pathological error conditions.
    */
  val ErrorHandler = "errorHandler"

  /** Option value: Majority Durability - to be used with [[Durability]] as the key.
    */
  val MajorityDurability = "majority"

  /** Option value: Majority And Persist To Active Durability - to be used with [[Durability]] as
    * the key.
    */
  val MajorityAndPersistToActiveDurability = "majorityAndPersistToActive"

  /** Option value: Persist To Majority Durability - to be used with [[Durability]] as the key.
    */
  val PersistToMajorityDurability = "PersistToMajority"

  /** Option value: Do not stream any additional metadata for a spark stream.
    */
  val StreamMetaDataNone = "none"

  /** Option value: Only stream basic metadata information.
    */
  val StreamMetaDataBasic = "basic"

  /** Option value: Stream all mutation metadata that is available.
    */
  val StreamMetaDataFull = "full"

  /** Option value: If an offset has been saved start from there, otherwise start the Structured
    * stream from "now" (no mutations are streamed from before this point).
    */
  val StreamFromNow = "fromNow"

  /** Option value: If an offset has been saved start from there, otherwise start the Structured
    * stream from "beginning" (previous mutations are streamed from before this point).
    */
  val StreamFromBeginning = "fromBeginning"

}
