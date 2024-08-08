/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.spark.columnar

import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.analytics.{AnalyticsOptions => CouchbaseAnalyticsOptions}
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.spark.columnar.ColumnarConstants.ColumnarEndpointIdx
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.{DefaultConstants, Keyspace}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class ColumnarPartition(id: Int, loc: Seq[String]) extends Partition {
  override def index: Int   = id
  def location: Seq[String] = loc
  override def toString     = s"ColumnarPartition($id, $loc)"
}

class ColumnarRDD[T: ClassTag](
    @transient private val sc: SparkContext,
    val statement: String,
    val columnarOptions: CouchbaseAnalyticsOptions = null,
    val keyspace: Keyspace = null,
    val connectionIdentifier: Option[String] = None
)(implicit deserializer: JsonDeserializer[T])
    extends RDD[T](sc, Nil)
    with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf, connectionIdentifier)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val connection = CouchbaseConnection(connectionIdentifier)
    val cluster    = connection.cluster(globalConfig)

    var options = if (this.columnarOptions == null) {
      CouchbaseAnalyticsOptions().endpointIdx(ColumnarEndpointIdx)
    } else {
      this.columnarOptions.endpointIdx(ColumnarEndpointIdx)
    }

    options = if (keyspace == null || keyspace.isEmpty) {
      options
    } else {
      options.raw(Map("query_context" -> ColumnarQueryContext.queryContext(keyspace.bucket.get, keyspace.scope.get)))
    }

    logInfo(s"Running Columnar query ${statement}")

    val result = cluster.analyticsQuery(statement, options).get

    logDebug(s"Metrics for columnar query $statement: " + result.metaData.metrics)
    result.rowsAs[T].get.iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val core   = CouchbaseConnection(connectionIdentifier).cluster(globalConfig).async.core
    val config = core.clusterConfig()

    val partitions = if (config.globalConfig() != null) {
      Array(
        new ColumnarPartition(
          0,
          config
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
            })
            .toSeq
        )
      )
    } else {
      Array(new ColumnarPartition(0, Seq()))
    }

    logDebug(
      s"Calculated ColumnarPartitions operation ${partitions.mkString("Array(", ", ", ")")}"
    )

    partitions.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[ColumnarPartition].location
  }

}
