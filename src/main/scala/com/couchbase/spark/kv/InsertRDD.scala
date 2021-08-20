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

import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.kv.{InsertOptions, MutationResult}
import com.couchbase.spark.{DefaultConstants, Keyspace}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import reactor.core.scala.publisher.SFlux

case class Insert[T](id: String, content: T)

class InsertRDD[T](@transient private val sc: SparkContext, val docs: Seq[Insert[T]], val keyspace: Keyspace,
                val insertOptions: InsertOptions = null)(implicit serializer: JsonSerializer[T])
  extends RDD[MutationResult](sc, Nil)
    with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)
  private val bucketName = globalConfig.implicitBucketNameOr(this.keyspace.bucket.orNull)

  override def compute(split: Partition, context: TaskContext): Iterator[MutationResult] = {
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
    val options = if (this.insertOptions == null) {
      InsertOptions()
    } else {
      this.insertOptions
    }

    logDebug(s"Performing bulk insert against ids ${partition.ids} with options $options")

    SFlux
      .fromIterable(docs)
      .filter(doc => partition.ids.contains(doc.id))
      .flatMap(doc => collection.insert(doc.id, doc.content, options))
      .collectSeq()
      .block()
      .iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = KeyValuePartition
      .partitionsForIds(this.docs.map(_.id), CouchbaseConnection(), globalConfig, bucketName)
      .asInstanceOf[Array[Partition]]

    logDebug(s"Calculated KeyValuePartitions for Insert operation ${partitions.mkString("Array(", ", ", ")")}")
    partitions
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[KeyValuePartition].location match {
      case Some(l) => Seq(l)
      case _ => Nil
    }
  }

}