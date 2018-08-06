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
    val addressesWithQueryService = couchbaseNodesWithQueryServices()

    // A single query can only run on one node, so return one partition
    Array(new QueryPartition(0, addressesWithQueryService))
  }

  private def couchbaseNodesWithQueryServices(): Seq[String] = {
    val core = CouchbaseConnection().bucket(cbConfig, bucketName).core()
    import collection.JavaConverters._

    val req = new GetClusterConfigRequest()
    val config = toScalaObservable(core.send[GetClusterConfigResponse](req))
      .toBlocking
      .single

    val addressesWithQueryService: Seq[String] = config.config().bucketConfigs().asScala
      .flatMap(v => {
        val bucketConfig = v._2
        val nodesWithQueryService = bucketConfig.nodes.asScala
          .filter(node => node.services()
            .asScala
            .contains(ServiceType.QUERY))
        nodesWithQueryService
      })
      .map(v => v.hostname().hostname())
      .toSeq
      .distinct

    addressesWithQueryService
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val p = split.asInstanceOf[QueryPartition]

    // If the user has co-located Spark worker services on Couchbase nodes, this will get the query
    // to run on a Spark worker running on a Couchbase query node, if possible
    val out = if (p.hostnames.nonEmpty) {
      p.hostnames
    } else {
      Nil
    }
    out
  }
}

object QueryRDD {

  def apply(sc: SparkContext, bucketName: String, query: N1qlQuery,
            timeout: Option[Duration] = None) =
    new QueryRDD(sc, query, bucketName, timeout)

}
