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

import com.couchbase.client.scala.kv.{GetOptions, GetResult}
import com.couchbase.spark.{DefaultConstants, Keyspace}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import reactor.core.scala.publisher.SFlux


class GetRDD(@transient private val sc: SparkContext, val ids: Seq[Get], val keyspace: Keyspace, getOptions: GetOptions = null)
  extends RDD[GetResult](sc, Nil)
    with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)
  private val bucketName = globalConfig.implicitBucketNameOr(this.keyspace.bucket.orNull)

  override def compute(split: Partition, context: TaskContext): Iterator[GetResult] = {
    val partition = split.asInstanceOf[KeyValuePartition]

    val connection = CouchbaseConnection()
    val cluster = connection.cluster(globalConfig)

    val scopeName = globalConfig
      .implicitScopeNameOr(this.keyspace.scope.orNull).
      getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = globalConfig
      .implicitCollectionName(this.keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (this.getOptions == null) {
      GetOptions()
    } else {
      this.getOptions
    }

    logDebug(s"Performing bulk get fetch against ids ${partition.ids} with options $options")

    SFlux
      .fromIterable(partition.ids)
      .flatMap(id => collection.get(id, options))
      .collectSeq()
      .block()
      .iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = KeyValuePartition
      .partitionsForIds(this.ids.map(_.id), CouchbaseConnection(), globalConfig, bucketName)
      .asInstanceOf[Array[Partition]]

    logDebug(s"Calculated KeyValuePartitions for Get operation ${partitions.mkString("Array(", ", ", ")")}")
    partitions
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[KeyValuePartition].location match {
      case Some(l) => Seq(l)
      case _ => Nil
    }
  }

}