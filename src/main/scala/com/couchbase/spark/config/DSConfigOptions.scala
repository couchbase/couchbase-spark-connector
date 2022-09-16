package com.couchbase.spark.config

import com.couchbase.spark.DefaultConstants

/**
 * Helper object to provide type-safe keys and values for Spark DataSource options.
 */
object DSConfigOptions {

  /**
   * Option Key: Connection String of the cluster.
   */
  val ConnectionString = "connectionString"

  /**
   * Option Key: Username to be used for cluster connection.
   */
  val Username = "username"

  /**
   * Option Key: Password to be used for cluster connection.
   */
  val Password = "password"

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

  /**
   * Option Key: a N1QL expression which acts as an additional filter on every query, including schema inference.
   *
   * This is usually needed in pre-collection clusters where a predicate needs to be used to always filter the
   * dataset. For example "type = 'airport'", but can be as complex as needed. This filter is always AND-combined
   * with any filters that get pushed down from spark when actually performing the query.
   */
  val Filter = "filter"

  /**
   * Option Key: Allows to override the scan consistency for the query performed.
   */
  val ScanConsistency = "scanConsistency"

  /**
   * Option Key: If aggregates should be allowed to be pushed down by spark into the query engine.
   *
   * This value is true by default for performance reasons. It is available to be disabled should there
   * be any issues identified in the field.
   */
  val PushDownAggregate = "pushDownAggregate"

  /**
   * Option Key: The name of the dataset.
   */
  val Dataset = "dataset"

  /**
   * Option Key: The limit of how many records to load during schema inference.
   *
   * The default can be found in [[DefaultConstants.DefaultInferLimit]]
   */
  val InferLimit = "inferLimit"

  /**
   * Option Value: Not bounded scan consistency - to be used with [[ScanConsistency]] as the key.
   *
   * This is the default and usually does not need to be specified. The query will be executed immediately, and
   * it might not include documents which have just been written and not made it into the index yet.
   */
  val NotBoundedScanConsistency = "notBounded"

  /**
   * Option Value: Request plus scan consistency - to be used with [[ScanConsistency]] as the key.
   *
   * If this value is set, the indexer will wait until it has caught up to the recent mutations, which will make sure
   * that documents which just have been written are part of the result set.
   */
  val RequestPlusScanConsistency = "requestPlus"

  val WaitUntilReadyTimeout = "waitUntilReadyTimeout"

}
