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

import com.couchbase.client.scala.kv.{LookupInOptions, LookupInResult, LookupInSpec}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection, CouchbaseConnectionPool}
import com.couchbase.spark.{DefaultConstants, Keyspace}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import reactor.core.scala.publisher.SFlux

class LookupInRDD(@transient private val sc: SparkContext, val docs: Seq[LookupIn], val keyspace: Keyspace, lookupInOptions: LookupInOptions = null)
  extends RDD[LookupInResult](sc, Nil)
    with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf,true)
  private val bucketName = globalConfig.implicitBucketNameOr(this.keyspace.bucket.orNull)

  override def compute(split: Partition, context: TaskContext): Iterator[LookupInResult] = {
    val partition = split.asInstanceOf[KeyValuePartition]
    val connection = CouchbaseConnectionPool().getConnection(globalConfig)
    val cluster = connection.cluster()

    val scopeName = globalConfig
      .implicitScopeNameOr(this.keyspace.scope.orNull).
      getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = globalConfig
      .implicitCollectionName(this.keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (this.lookupInOptions == null) {
      LookupInOptions()
    } else {
      this.lookupInOptions
    }

    logDebug(s"Performing bulk LookupIn fetch against ids ${partition.ids} with options $options")

    SFlux
      .fromIterable(docs)
      .filter(doc => partition.ids.contains(doc.id))
      .flatMap(doc => collection.lookupIn(doc.id, doc.specs, options))
      .collectSeq()
      .block()
      .iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = KeyValuePartition
      .partitionsForIds(docs.map(d => d.id), CouchbaseConnectionPool().getConnection(globalConfig), globalConfig, bucketName)
      .asInstanceOf[Array[Partition]]

    logDebug(s"Calculated KeyValuePartitions for LookupIn operation ${partitions.mkString("Array(", ", ", ")")}")
    partitions
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[KeyValuePartition].location match {
      case Some(l) => Seq(l)
      case _ => Nil
    }
  }

}