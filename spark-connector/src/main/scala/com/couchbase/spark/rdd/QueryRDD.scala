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
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection, QueryAccessor}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import rx.lang.scala.JavaConversions.toScalaObservable

import scala.concurrent.duration.Duration

case class CouchbaseQueryRow(value: JsonObject)

class QueryPartition(val index: Int, val hostnames: Seq[String]) extends Partition {
  override def toString = s"QueryPartition($index, $hostnames)"
}

class QueryRDD(@transient private val sc: SparkContext, query: N1qlQuery,
               bucketName: String = null,
               timeout: Option[Duration] = None)
  extends RDD[CouchbaseQueryRow](sc, Nil) {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseQueryRow] =
    new QueryAccessor(cbConfig, Seq(query), bucketName, timeout).compute()

  override protected def getPartitions: Array[Partition] = {
    // Try to run the query on a Spark worker co-located on a Couchbase query node
    val addressesWithQueryService = RDDSupport.couchbaseNodesWithService(cbConfig,
      bucketName,
      ServiceType.QUERY)

    // A single query can only run on one node, so return one partition
    Array(new QueryPartition(0, addressesWithQueryService))
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    RDDSupport.getPreferredLocations(split)
  }
}

object QueryRDD {

  def apply(sc: SparkContext, bucketName: String, query: N1qlQuery,
            timeout: Option[Duration] = None) =
    new QueryRDD(sc, query, bucketName, timeout)

}
