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

package com.couchbase.spark.rdd

import com.couchbase.client.core.config.CouchbaseBucketConfig
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{GetOptions, GetResult}
import com.couchbase.spark.core.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import reactor.core.scala.publisher.{SFlux, SMono}

import java.nio.charset.StandardCharsets
import java.util.zip.CRC32
import scala.compat.java8.OptionConverters
import java.time.Instant
import scala.concurrent.duration.Duration

class CouchbaseGetRDD(
  @transient private val sc: SparkContext,
  private[spark] val documentIds: Seq[String],
  private[spark] val options: CouchbaseGetOptions = CouchbaseGetOptions(),
) extends RDD[CouchbaseGetResult](sc, Nil) {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseGetResult] = {
    val connection = CouchbaseConnection()
    val collection = connection
      .collection(globalConfig, options.bucketName, options.scopeName, options.collectionName)

    val p = split.asInstanceOf[KeyValuePartition]
    SFlux
      .fromIterable(p.ids)
      .flatMap(id => collection.reactive
        .get(id, options.toGetOptions)
        .onErrorResume(e => {
          if (e.isInstanceOf[DocumentNotFoundException]) {
             SMono.empty[GetResult]
          } else {
             SMono.raiseError[GetResult](e)
          }
        })
      )
      .map(r => CouchbaseGetResult(r.id, r.cas, r.expiryTime, r.contentAs[JsonObject].get))
      .toStream()
      .iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val connection = CouchbaseConnection()
    val core = connection.cluster(globalConfig).async.core

    val bucketName = connection.bucketName(globalConfig, options.bucketName)
    val bucketConfig = core.clusterConfig().bucketConfig(bucketName)

    val parts = bucketConfig match {
      case bucketConfig: CouchbaseBucketConfig => {
        val numPartitions = bucketConfig.numberOfPartitions()
        var partitionIndex = 0
        documentIds.groupBy(id => {
          val crc32 = new CRC32()
          crc32.update(id.getBytes(StandardCharsets.UTF_8))
          val rv = (crc32.getValue >> 16) & 0x7fff
          rv.toInt & numPartitions - 1
        }).map(grouped => {
          val nodeInfo = bucketConfig.nodeAtIndex(
            bucketConfig.nodeIndexForActive(grouped._1, false)
          )
          val hostname = OptionConverters.toScala(core.context().alternateAddress()) match {
            case Some(a) => nodeInfo.alternateAddresses().get(a).hostname()
            case None => nodeInfo.hostname()
          }
          val currentIdx = partitionIndex
          partitionIndex += 1
          new KeyValuePartition(currentIdx, grouped._2, Some(hostname))
        }).toArray
      }
      case _ => {
        Array(new KeyValuePartition(0, documentIds, None))
      }
    }

    parts.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
      split.asInstanceOf[KeyValuePartition].location match {
      case Some(l) => Seq(l)
      case None => Nil
    }
  }

}

case class CouchbaseGetOptions(
  bucketName: Option[String] = None,
  scopeName: Option[String] = None,
  collectionName: Option[String] = None,
  timeout: Option[Duration] = None,
  retryStrategy: Option[RetryStrategy] = None,
  withExpiry: Boolean = false,
  project: Seq[String] = Seq.empty,
) {
  def toGetOptions: GetOptions = {
    var opts = GetOptions()

    if (timeout.isDefined) {
      opts = opts.timeout(timeout.get)
    }

    if (retryStrategy.isDefined) {
      opts = opts.retryStrategy(retryStrategy.get)
    }

    if (withExpiry) {
      opts = opts.withExpiry(withExpiry)
    }

    if (project.nonEmpty) {
      opts = opts.project(project)
    }

    opts
  }
}

case class CouchbaseGetResult(id: String, cas: Long, expiryTime: Option[Instant], content: JsonObject)