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
package com.couchbase.spark

import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.kv.{
  InsertOptions,
  MutateInOptions,
  MutateInResult,
  MutationResult,
  RemoveOptions,
  ReplaceOptions,
  UpsertOptions
}
import com.couchbase.spark.config.CouchbaseConfig
import com.couchbase.spark.kv.{Insert, KeyValueOperationRunner, MutateIn, Remove, Replace, Upsert}
import org.apache.spark.rdd.RDD
import com.couchbase.client.core.error.{DocumentExistsException, DocumentNotFoundException}

/** Functions which can be performed on an RDD if the evidence matches.
  *
  * @param rdd
  *   the rdd on which the operations are performed on.
  * @tparam T
  *   the generic RDD type to operate on.
  */
class RDDFunctions[T](rdd: RDD[T]) extends Serializable {

  /** Upserts documents into couchbase.
    *
    * @param keyspace
    *   the optional keyspace to override the implicit configuration.
    * @param upsertOptions
    *   optional parameters to customize the behavior.
    * @param evidence
    *   the generic type for which this method is made available on the RDD.
    * @param serializer
    *   the JSON serializer to use when encoding the documents.
    * @tparam V
    *   the generic type to use.
    * @return
    *   the RDD result.
    */
  def couchbaseUpsert[V](
      keyspace: Keyspace = Keyspace(),
      upsertOptions: UpsertOptions = null,
      connectionIdentifier: String = null
  )(implicit
      evidence: RDD[T] <:< RDD[Upsert[V]],
      serializer: JsonSerializer[V]
  ): RDD[MutationResult] = {
    val rdd: RDD[Upsert[V]] = this.rdd

    val ci     = Option(connectionIdentifier)
    val config = CouchbaseConfig(rdd.sparkContext.getConf, ci)

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutationResult]()
      } else {
        KeyValueOperationRunner
          .upsert(config, keyspace, iterator.toSeq, upsertOptions, ci)
          .iterator
      }
    }
  }

  /** Inserts documents into couchbase.
    *
    * @param keyspace
    *   the optional keyspace to override the implicit configuration.
    * @param insertOptions
    *   optional parameters to customize the behavior.
    * @param ignoreIfExists
    *   set to true if individual [[DocumentExistsException]] should be ignored.
    * @param evidence
    *   the generic type for which this method is made available on the RDD.
    * @param serializer
    *   the JSON serializer to use when encoding the documents.
    * @tparam V
    *   the generic type to use.
    * @return
    *   the RDD result.
    */
  def couchbaseInsert[V](
      keyspace: Keyspace = Keyspace(),
      insertOptions: InsertOptions = null,
      ignoreIfExists: Boolean = false,
      connectionIdentifier: String = null
  )(implicit
      evidence: RDD[T] <:< RDD[Insert[V]],
      serializer: JsonSerializer[V]
  ): RDD[MutationResult] = {
    val rdd: RDD[Insert[V]] = this.rdd

    val ci     = Option(connectionIdentifier)
    val config = CouchbaseConfig(rdd.sparkContext.getConf, ci)

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutationResult]()
      } else {
        KeyValueOperationRunner
          .insert(config, keyspace, iterator.toSeq, insertOptions, ignoreIfExists, ci)
          .iterator
      }
    }
  }

  /** Replaces documents in couchbase.
    *
    * @param keyspace
    *   the optional keyspace to override the implicit configuration.
    * @param replaceOptions
    *   optional parameters to customize the behavior.
    * @param ignoreIfNotFound
    *   set to true if individual [[DocumentNotFoundException]] should be ignored.
    * @param evidence
    *   the generic type for which this method is made available on the RDD.
    * @param serializer
    *   the JSON serializer to use when encoding the documents.
    * @tparam V
    *   the generic type to use.
    * @return
    *   the RDD result.
    */
  def couchbaseReplace[V](
      keyspace: Keyspace = Keyspace(),
      replaceOptions: ReplaceOptions = null,
      ignoreIfNotFound: Boolean = false,
      connectionIdentifier: String = null
  )(implicit
      evidence: RDD[T] <:< RDD[Replace[V]],
      serializer: JsonSerializer[V]
  ): RDD[MutationResult] = {
    val rdd: RDD[Replace[V]] = this.rdd

    val ci     = Option(connectionIdentifier)
    val config = CouchbaseConfig(rdd.sparkContext.getConf, ci)

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutationResult]()
      } else {
        KeyValueOperationRunner
          .replace(config, keyspace, iterator.toSeq, replaceOptions, ignoreIfNotFound, ci)
          .iterator
      }
    }
  }

  /** Removes documents from couchbase.
    *
    * @param keyspace
    *   the optional keyspace to override the implicit configuration.
    * @param removeOptions
    *   optional parameters to customize the behavior.
    * @param ignoreIfNotFound
    *   set to true if individual [[DocumentNotFoundException]] should be ignored.
    * @param evidence
    *   the generic type for which this method is made available on the RDD.
    * @return
    *   the RDD result.
    */
  def couchbaseRemove(
      keyspace: Keyspace = Keyspace(),
      removeOptions: RemoveOptions = null,
      ignoreIfNotFound: Boolean = false,
      connectionIdentifier: String = null
  )(implicit evidence: RDD[T] <:< RDD[Remove]): RDD[MutationResult] = {
    val rdd: RDD[Remove] = this.rdd

    val ci     = Option(connectionIdentifier)
    val config = CouchbaseConfig(rdd.sparkContext.getConf, ci)

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutationResult]()
      } else {
        KeyValueOperationRunner
          .remove(config, keyspace, iterator.toSeq, removeOptions, ignoreIfNotFound, ci)
          .iterator
      }
    }
  }

  /** Mutates sub-documents in couchbase.
    *
    * @param keyspace
    *   the optional keyspace to override the implicit configuration.
    * @param mutateInOptions
    *   optional parameters to customize the behavior.
    * @param evidence
    *   the generic type for which this method is made available on the RDD.
    * @return
    *   the RDD result.
    */
  def couchbaseMutateIn(
      keyspace: Keyspace = Keyspace(),
      mutateInOptions: MutateInOptions = null,
      connectionIdentifier: String = null
  )(implicit
      evidence: RDD[T] <:< RDD[MutateIn]
  ): RDD[MutateInResult] = {
    val rdd: RDD[MutateIn] = this.rdd

    val ci     = Option(connectionIdentifier)
    val config = CouchbaseConfig(rdd.sparkContext.getConf, ci)

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutateInResult]()
      } else {
        KeyValueOperationRunner
          .mutateIn(config, keyspace, iterator.toSeq, mutateInOptions, ci)
          .iterator
      }
    }
  }

}
