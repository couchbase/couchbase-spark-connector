/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.spark.rdd

import com.couchbase.client.core.message.cluster.{GetClusterConfigRequest, GetClusterConfigResponse}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.java.analytics.AnalyticsQuery
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark.connection._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import rx.lang.scala.JavaConversions.toScalaObservable

import scala.concurrent.duration.Duration

/**
  * Wraps a Couchbase Analytics query in an RDD
  */
class AnalyticsRDD(@transient private val sc: SparkContext, query: AnalyticsQuery,
                   bucketName: String = null,
                   timeout: Option[Duration] = None)
  extends RDD[CouchbaseAnalyticsRow](sc, Nil) {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseAnalyticsRow] =
    new AnalyticsAccessor(cbConfig, Seq(query), bucketName, timeout).compute()

  override protected def getPartitions: Array[Partition] = {
    // Try to run the query on a Spark worker co-located on a Couchbase analytics node
    val addressesWithAnalyticsService = RDDSupport.couchbaseNodesWithService(cbConfig,
      bucketName,
      ServiceType.ANALYTICS)

    // A single query can only run on one node, so return one partition
    Array(new QueryPartition(0, addressesWithAnalyticsService))
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    RDDSupport.getPreferredLocations(split)
  }
}


