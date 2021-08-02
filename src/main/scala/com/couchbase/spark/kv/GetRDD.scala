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
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

class GetRDD(@transient private val sc: SparkContext, val ids: Seq[String], bucket: Option[String] = None,
             scope: Option[String] = None, collection: Option[String] = None, getOptions: GetOptions = null)
  extends RDD[GetResult](sc, Nil)
    with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[GetResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(globalConfig)

    val bucketName = globalConfig.implicitBucketNameOr(this.bucket.orNull)
    val scopeName = globalConfig
      .implicitScopeNameOr(this.scope.orNull).
      getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = globalConfig
      .implicitCollectionName(this.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName)
    val options = if (this.getOptions == null) {
      GetOptions()
    } else {
      this.getOptions
    }
    ids.map(id => collection.get(id, options).get).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(KeyValuePartition(0))
  }

  case class KeyValuePartition(index: Int) extends Partition()
}