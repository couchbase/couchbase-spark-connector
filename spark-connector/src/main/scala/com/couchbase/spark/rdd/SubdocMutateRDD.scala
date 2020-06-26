/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import java.net.InetAddress
import java.util.zip.CRC32

import com.couchbase.client.core.config.CouchbaseBucketConfig
import com.couchbase.client.core.message.cluster.{GetClusterConfigRequest, GetClusterConfigResponse}
import com.couchbase.spark.connection._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import rx.lang.scala.JavaConversions._

import scala.concurrent.duration.Duration

class SubdocMutationPartition(id: Int, specs: Seq[SubdocMutationSpec], loc: Option[InetAddress])
  extends Partition {
  override def index: Int = id
  def ids: Seq[SubdocMutationSpec] = specs
  def location: Option[InetAddress] = loc
  override def toString = s"SubdocMutatePartition($id, $ids, $loc)"
}

class SubdocMutateRDD(@transient private val sc: SparkContext, specs: Seq[SubdocMutationSpec],
                      bname: String = null, timeout: Option[Duration] = None)
  extends RDD[SubdocMutationResult](sc, Nil) {

  private val cbConfig = CouchbaseConfig(sc.getConf)
  private val bucketName = Option(bname).getOrElse(cbConfig.buckets.head.name)


  override def compute(split: Partition, context: TaskContext): Iterator[SubdocMutationResult] = {
    val p = split.asInstanceOf[SubdocMutationPartition]
    new SubdocMutationAccessor(cbConfig, p.ids, bucketName, timeout).compute()
  }

  override protected def getPartitions: Array[Partition] = {
    val core = CouchbaseConnection().bucket(cbConfig, bucketName).core()

    val req = new GetClusterConfigRequest()
    val config = toScalaObservable(core.send[GetClusterConfigResponse](req))
      .map(c => {
        logWarning(c.config().bucketConfigs().toString)
        logWarning(bucketName)
        c.config().bucketConfig(bucketName)
      })
      .toBlocking
      .single

    val parts = config match {
      case bucketConfig: CouchbaseBucketConfig =>
        val numPartitions = bucketConfig.numberOfPartitions()
        var partitionIndex = 0
        specs.groupBy(spec => {
          val crc32 = new CRC32()
          crc32.update(spec.id.getBytes("UTF-8"))
          val rv = (crc32.getValue >> 16) & 0x7fff
          rv.toInt & numPartitions - 1
        }).map(grouped => {
          val hostname = Some(
            bucketConfig.nodeAtIndex(bucketConfig.nodeIndexForMaster(grouped._1, false)).hostname()
          )
          val currentIdx = partitionIndex
          partitionIndex += 1
          new SubdocMutationPartition(currentIdx, grouped._2,
            Some(InetAddress.getByName(hostname.get)))
        }).toArray
      case _ =>
        logWarning("Memcached preferred locations currently not supported.")
        Array(new SubdocMutationPartition(0, specs, None))
    }

    parts.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val p = split.asInstanceOf[SubdocMutationPartition]
    if (p.location.isDefined) {
      Seq(p.location.get.getHostName, p.location.get.getHostAddress)
    } else {
      Nil
    }
  }

}
