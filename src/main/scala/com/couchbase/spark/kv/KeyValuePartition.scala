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
package com.couchbase.spark.kv

import com.couchbase.client.core.config.CouchbaseBucketConfig
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.Partition

import java.util.zip.CRC32

class KeyValuePartition(id: Int, docIds: Seq[String], loc: Option[String]) extends Partition {
  override def index: Int      = id
  def ids: Seq[String]         = docIds
  def location: Option[String] = loc
  override def toString        = s"KeyValuePartition($id, $docIds, $loc)"
}

object KeyValuePartition {

  def partitionsForIds(
      ids: Seq[String],
      connection: CouchbaseConnection,
      couchbaseConfig: CouchbaseConfig,
      bucketName: String
  ): Array[KeyValuePartition] = {
    val core   = connection.cluster(couchbaseConfig).async.core
    val _      = connection.bucket(couchbaseConfig, Some(bucketName))
    val config = core.clusterConfig().bucketConfig(bucketName)

    config match {
      case bucketConfig: CouchbaseBucketConfig =>
        val numPartitions  = bucketConfig.numberOfPartitions()
        var partitionIndex = 0
        ids
          .groupBy(id => {
            val crc32 = new CRC32()
            crc32.update(id.getBytes("UTF-8"))
            val rv = (crc32.getValue >> 16) & 0x7fff
            rv.toInt & numPartitions - 1
          })
          .map(grouped => {
            val nodeInfo =
              bucketConfig.nodeAtIndex(bucketConfig.nodeIndexForActive(grouped._1, false))
            val aa = core.context().alternateAddress()
            val hostname = if (aa != null && aa.isPresent) {
              nodeInfo.alternateAddresses().get(aa.get()).hostname()
            } else {
              nodeInfo.hostname()
            }

            val currentIdx = partitionIndex
            partitionIndex += 1
            new KeyValuePartition(currentIdx, grouped._2, Some(hostname))
          })
          .toArray
      case _ =>
        Array(new KeyValuePartition(0, ids, None))
    }
  }
}
