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


import java.net.InetAddress
import java.util.zip.CRC32

import com.couchbase.client.core.config.CouchbaseBucketConfig
import com.couchbase.client.core.message.cluster.{GetClusterConfigRequest, GetClusterConfigResponse}
import com.couchbase.client.java.document.Document
import com.couchbase.spark.Logging
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection, KeyValueAccessor}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag
import rx.lang.scala.JavaConversions._

import scala.concurrent.duration.Duration

class KeyValuePartition(id: Int, docIds: Seq[String], loc: Option[InetAddress]) extends Partition {
  override def index: Int = id
  def ids: Seq[String] = docIds
  def location: Option[InetAddress] = loc
  override def toString = s"KeyValuePartition($id, $docIds, $loc)"
}

class KeyValueRDD[D <: Document[_]]
  (@transient private val sc: SparkContext, ids: Seq[String], bname: String = null,
   timeout: Option[Duration] = None)
  (implicit ct: ClassTag[D])
  extends RDD[D](sc, Nil) {

  private val cbConfig = CouchbaseConfig(sc.getConf)
  private val bucketName = Option(bname).getOrElse(cbConfig.buckets.head.name)

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    val p = split.asInstanceOf[KeyValuePartition]
    new KeyValueAccessor[D](cbConfig, p.ids, bucketName, timeout).compute()
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
        ids.groupBy(id => {
          val crc32 = new CRC32()
          crc32.update(id.getBytes("UTF-8"))
          val rv = (crc32.getValue >> 16) & 0x7fff
          rv.toInt & numPartitions - 1
        }).map(grouped => {
          val hostname = Some(
            bucketConfig.nodeAtIndex(bucketConfig.nodeIndexForMaster(grouped._1, false)).hostname()
          )
          val currentIdx = partitionIndex
          partitionIndex += 1
          new KeyValuePartition(currentIdx, grouped._2,
            Some(InetAddress.getByName(hostname.get.address())))
        }).toArray
      case _ =>
        logWarning("Memcached preferred locations currently not supported.")
        Array(new KeyValuePartition(0, ids, None))
    }

    parts.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val p = split.asInstanceOf[KeyValuePartition]
    if (p.location.isDefined) {
      Seq(p.location.get.getHostName, p.location.get.getHostAddress)
    } else {
      Nil
    }
  }

}
