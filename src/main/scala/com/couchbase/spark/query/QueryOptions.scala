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
package com.couchbase.spark.query

import com.couchbase.spark.DefaultConstants

/** Helper object to provide type-safe keys and values for Spark SQL query options.
  */
object QueryOptions {

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

  /** Option Key: a N1QL expression which acts as an additional filter on every query, including
    * schema inference.
    *
    * This is usually needed in pre-collection clusters where a predicate needs to be used to always
    * filter the dataset. For example "type = 'airport'", but can be as complex as needed. This
    * filter is always AND-combined with any filters that get pushed down from spark when actually
    * performing the query.
    */
  val Filter = "filter"

  /** Option Key: The field name of the document ID, used to override the default.
    *
    * The default can be located in [[DefaultConstants.DefaultIdFieldName]]
    */
  val IdFieldName = "idFieldName"

  /** Option Key: Allows to override the scan consistency for the query performed.
    */
  val ScanConsistency = "scanConsistency"

  /** Option Key: The limit of how many records to load during schema inference.
    *
    * The default can be found in [[DefaultConstants.DefaultInferLimit]]
    */
  val InferLimit = "inferLimit"

  /** Option Key: The timeout to use which overrides the default configured.
    *
    * The value is a string and must be parsable from a scala Duration.
    */
  val Timeout = "timeout"

  /** Option Key: If aggregates should be allowed to be pushed down by spark into the query engine.
    *
    * This value is true by default for performance reasons. It is available to be disabled should
    * there be any issues identified in the field.
    */
  val PushDownAggregate = "pushDownAggregate"

  /** Option Value: Not bounded scan consistency - to be used with [[ScanConsistency]] as the key.
    *
    * This is the default and usually does not need to be specified. The query will be executed
    * immediately, and it might not include documents which have just been written and not made it
    * into the index yet.
    */
  val NotBoundedScanConsistency = "notBounded"

  /** Option Value: Request plus scan consistency - to be used with [[ScanConsistency]] as the key.
    *
    * If this value is set, the indexer will wait until it has caught up to the recent mutations,
    * which will make sure that documents which just have been written are part of the result set.
    */
  val RequestPlusScanConsistency = "requestPlus"

  /** Option Values: Partitioning.
    *
    * If any of the four following options are set, all must be.
    *
    * They will partition a single SQL++ query into multiple queries, so it can be executed by multiple Spark workers.
    *
    * As an example, if partitionColumn is set to "id", partitionLowerBound is 0, partitionUpperBound is 1000, and
    * partitionCount is 3, then the multiple queries would look similar to this:
    *
    * SELECT [...] WHERE [...] AND id < 334
    * SELECT [...] WHERE [...] AND (id >= 334 AND id < 667)
    * SELECT [...] WHERE [...] AND id >= 667
    *
    * The chosen partitionColumn must support the SQL comparison operators "<" and ">=".
    * Any results where partitionColumn is null or otherwise not matched by those operators, will not be included.
    *
    * partitionLowerBound and partitionUpperBound do not bound or limit the results, they simply choose how many results
    * are in each partition.
    *
    * partitionCount determines how many partitions the query will be split into.
    */
  val PartitionColumn = "partitionColumn"
  val PartitionLowerBound = "partitionLowerBound"
  val PartitionUpperBound = "partitionUpperBound"
  val PartitionCount = "partitionCount"
}
