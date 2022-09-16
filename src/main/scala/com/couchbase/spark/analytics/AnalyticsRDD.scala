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
package com.couchbase.spark.analytics

import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.analytics.{AnalyticsOptions => CouchbaseAnalyticsOptions}
import com.couchbase.spark.{DefaultConstants, Keyspace}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection, CouchbaseConnectionPool}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag
import collection.JavaConverters._
import java.util.{Map,HashMap}

import com.couchbase.spark.config.mapToSparkConf

class AnalyticsPartition(id: Int, loc: Seq[String]) extends Partition {
  override def index: Int = id
  def location: Seq[String] = loc
  override def toString = s"AnalyticsPartition($id, $loc)"
}

class AnalyticsRDD[T: ClassTag](
  @transient private val sc: SparkContext,
  val statement: String,
  val analyticsOptions: CouchbaseAnalyticsOptions = null,
  val keyspace: Keyspace = null,
  val connectionOptions: Map[String,String] = new HashMap[String,String]()
)(implicit deserializer: JsonDeserializer[T]) extends RDD[T](sc, Nil) with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf,false).loadDSOptions(connectionOptions)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val connection = CouchbaseConnectionPool().getConnection(globalConfig)
    val cluster = connection.cluster()

    val options = if (this.analyticsOptions == null) {
      CouchbaseAnalyticsOptions()
    } else {
      this.analyticsOptions
    }

    val result = if (keyspace == null || keyspace.isEmpty) {
      cluster.analyticsQuery(statement, options).get
    } else {
      if (keyspace.collection.isDefined) {
        throw new IllegalArgumentException("A Collection must not be provided on an Analytics Query inside the Keyspace, " +
          "only Bucket and/or Scope are allowed. The collection itself is provided as part of the statement itself!")
      }

      val bucketName = globalConfig.
        implicitBucketNameOr(this.keyspace.bucket.orNull)

      val scopeName = globalConfig
        .implicitScopeNameOr(this.keyspace.scope.orNull).
        getOrElse(DefaultConstants.DefaultScopeName)

      cluster.bucket(bucketName).scope(scopeName).analyticsQuery(statement, options).get
    }

    logDebug(s"Metrics for analytics query $statement: " + result.metaData.metrics)
    result.rowsAs[T].get.iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val core = CouchbaseConnectionPool().getConnection(globalConfig).cluster().async.core
    val config = core.clusterConfig()

    val partitions = if (config.globalConfig() != null) {
      Array(new AnalyticsPartition(0, config
        .globalConfig()
        .portInfos()
        .asScala
        .filter(p => p.ports().containsKey(ServiceType.ANALYTICS))
        .map(p => {
          val aa = core.context().alternateAddress()
          if (aa != null && aa.isPresent) {
            p.alternateAddresses().get(aa.get()).hostname()
          } else {
            p.hostname()
          }
        })))
    } else {
      Array(new AnalyticsPartition(0, Seq())
      )
    }

    logDebug(s"Calculated AnalyticsPartitions operation ${partitions.mkString("Array(", ", ", ")")}")

    partitions.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[AnalyticsPartition].location
  }

}