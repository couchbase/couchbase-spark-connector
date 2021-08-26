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
import com.couchbase.client.scala.kv.{InsertOptions, MutateInOptions, MutateInResult, MutationResult, RemoveOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.spark.config.CouchbaseConfig
import com.couchbase.spark.kv.{Insert, KeyValueOperationRunner, MutateIn, Remove, Replace, Upsert}
import org.apache.spark.rdd.RDD

class RDDFunctions[T](rdd: RDD[T]) extends Serializable {

  private val config = CouchbaseConfig(rdd.sparkContext.getConf)

  def couchbaseUpsert[V](keyspace: Keyspace = Keyspace(), upsertOptions: UpsertOptions = null)
                        (implicit evidence: RDD[T] <:< RDD[Upsert[V]], serializer: JsonSerializer[V]) : RDD[MutationResult] = {
    val rdd: RDD[Upsert[V]] = this.rdd

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutationResult]()
      } else {
        KeyValueOperationRunner.upsert(config, keyspace, iterator.toSeq, upsertOptions).iterator
      }
    }
  }

  def couchbaseInsert[V](keyspace: Keyspace = Keyspace(), insertOptions: InsertOptions = null)
                        (implicit evidence: RDD[T] <:< RDD[Insert[V]], serializer: JsonSerializer[V]) : RDD[MutationResult] = {
    val rdd: RDD[Insert[V]] = this.rdd

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutationResult]()
      } else {
        KeyValueOperationRunner.insert(config, keyspace, iterator.toSeq, insertOptions).iterator
      }
    }
  }

  def couchbaseReplace[V](keyspace: Keyspace = Keyspace(), replaceOptions: ReplaceOptions = null)
                         (implicit evidence: RDD[T] <:< RDD[Replace[V]], serializer: JsonSerializer[V]) : RDD[MutationResult] = {
    val rdd: RDD[Replace[V]] = this.rdd

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutationResult]()
      } else {
        KeyValueOperationRunner.replace(config, keyspace, iterator.toSeq, replaceOptions).iterator
      }
    }
  }

  def couchbaseRemove(keyspace: Keyspace = Keyspace(), removeOptions: RemoveOptions = null)
                     (implicit evidence: RDD[T] <:< RDD[Remove]) : RDD[MutationResult] = {
    val rdd: RDD[Remove] = this.rdd

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutationResult]()
      } else {
        KeyValueOperationRunner.remove(config, keyspace, iterator.toSeq, removeOptions).iterator
      }
    }
  }

  def couchbaseMutateIn(keyspace: Keyspace = Keyspace(), mutateInOptions: MutateInOptions = null)
                     (implicit evidence: RDD[T] <:< RDD[MutateIn]) : RDD[MutateInResult] = {
    val rdd: RDD[MutateIn] = this.rdd

    rdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator[MutateInResult]()
      } else {
        KeyValueOperationRunner.mutateIn(config, keyspace, iterator.toSeq, mutateInOptions).iterator
      }
    }
  }


}
