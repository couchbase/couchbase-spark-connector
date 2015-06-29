/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark.rdd


import java.net.InetAddress
import java.util.zip.CRC32

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.core.config.{CouchbaseBucketConfig, BucketType}
import com.couchbase.client.core.message.cluster.{GetClusterConfigRequest, GetClusterConfigResponse}
import com.couchbase.client.java.document.Document
import com.couchbase.spark.connection.{CouchbaseConnection, KeyValueAccessor, CouchbaseConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, Logging, SparkContext}

import scala.reflect.ClassTag

import rx.lang.scala.JavaConversions._

class KeyValuePartition(id: Int, docIds: Seq[String], loc: Option[InetAddress]) extends Partition {
  override def index: Int = id
  def ids: Seq[String] = docIds
  def location: Option[InetAddress] = loc
  override def toString = s"KeyValuePartition($id, $docIds, $loc)"
}

class KeyValueRDD[D <: Document[_]]
  (@transient sc: SparkContext, ids: Seq[String], bname: String = null)
  (implicit ct: ClassTag[D])
  extends RDD[D](sc, Nil)
  with Logging {

  private val cbConfig = CouchbaseConfig(sc.getConf)
  private val bucketName = Option(bname).getOrElse(cbConfig.buckets.head.name)

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    val p = split.asInstanceOf[KeyValuePartition]
    new KeyValueAccessor[D](cbConfig, p.ids, bucketName).compute()
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


    val parts = if (config.isInstanceOf[CouchbaseBucketConfig]) {
      val bucketConfig = config.asInstanceOf[CouchbaseBucketConfig]
      val numPartitions = bucketConfig.numberOfPartitions()
      ids.groupBy(id => {
        val crc32 = new CRC32()
        crc32.update(id.getBytes("UTF-8"))
        val rv = (crc32.getValue >> 16) & 0x7fff
        rv.toInt & numPartitions - 1
      }).map(grouped => {
        val hostname = Some(
          bucketConfig.nodeAtIndex(bucketConfig.nodeIndexForMaster(grouped._1)).hostname()
        )
        new KeyValuePartition(grouped._1, grouped._2, hostname)
      }).toArray
    } else {
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
