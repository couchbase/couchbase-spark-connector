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

import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection, CouchbaseConnectionPool}
import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import com.couchbase.client.scala.query.{QueryOptions => CouchbaseQueryOptions}
import com.couchbase.spark.{DefaultConstants, Keyspace}

import collection.JavaConverters._
import scala.reflect.ClassTag

class QueryPartition(id: Int, loc: Seq[String]) extends Partition {
  override def index: Int = id
  def location: Seq[String] = loc
  override def toString = s"QueryPartition($id, $loc)"
}

class QueryRDD[T: ClassTag](
  @transient private val sc: SparkContext,
  val statement: String,
  val queryOptions: CouchbaseQueryOptions = null,
  val keyspace: Keyspace = null,
)(implicit deserializer: JsonDeserializer[T]) extends RDD[T](sc, Nil) with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf,true)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val connection = CouchbaseConnectionPool().getConnection(globalConfig)
    val cluster = connection.cluster()

    var options = if (this.queryOptions == null) {
      CouchbaseQueryOptions()
    } else {
      this.queryOptions
    }
    options = options.metrics(true)

    val result = if (keyspace == null || keyspace.isEmpty) {
      cluster.query(statement, options).get
    } else {
      if (keyspace.collection.isDefined) {
        throw new IllegalArgumentException("A Collection must not be provided on a N1QL Query inside the Keyspace, " +
          "only Bucket and/or Scope are allowed. The collection itself is provided as part of the statement itself!")
      }

      val bucketName = globalConfig.
        implicitBucketNameOr(this.keyspace.bucket.orNull)

      val scopeName = globalConfig
        .implicitScopeNameOr(this.keyspace.scope.orNull).
        getOrElse(DefaultConstants.DefaultScopeName)

      cluster.bucket(bucketName).scope(scopeName).query(statement, options).get
    }

    if (result.metaData.metrics.isDefined) {
      logDebug(s"Metrics for query $statement: " + result.metaData.metrics.get)
    }
    result.rowsAs[T].get.iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val core = CouchbaseConnectionPool().getConnection(globalConfig).cluster().async.core
    val config = core.clusterConfig()

    val partitions = if (config.globalConfig() != null) {
      Array(new QueryPartition(0, config
        .globalConfig()
        .portInfos()
        .asScala
        .filter(p => p.ports().containsKey(ServiceType.QUERY))
        .map(p => {
          val aa = core.context().alternateAddress()
          if (aa != null && aa.isPresent) {
            p.alternateAddresses().get(aa.get()).hostname()
          } else {
            p.hostname()
          }
        })))
    } else {
      Array(new QueryPartition(0, Seq())
      )
    }

    logDebug(s"Calculated QueryPartitions  operation ${partitions.mkString("Array(", ", ", ")")}")

    partitions.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[QueryPartition].location
  }

}