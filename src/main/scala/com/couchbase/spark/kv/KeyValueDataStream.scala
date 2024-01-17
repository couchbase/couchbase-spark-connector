/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.client.dcp.highlevel.{SnapshotMarker, StreamOffset}
import com.couchbase.client.dcp.message.DcpFailoverLogResponse
import com.couchbase.client.dcp.Client
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.util.Version
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.streaming.{Offset, PartitionOffset, SparkDataStream}
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, NoTypeHints}

import scala.collection.JavaConverters._
import scala.collection.mutable

class KeyValueDataStream(config: KeyValueStreamConfig, checkpointLocation: String)
    extends SparkDataStream
    with Logging {

  implicit val defaultFormats: DefaultFormats = DefaultFormats

  private lazy val sparkSession = SparkSession.active
  lazy val conf: CouchbaseConfig =
    CouchbaseConfig(sparkSession.sparkContext.getConf, config.connectionIdentifier)

  val dcpClient: Client = Client
    .builder()
    .seedNodes(
      CouchbaseConnection(config.connectionIdentifier).dcpSeedNodes(
        conf,
        config.connectionIdentifier
      )
    )
    .bucket(config.bucket)
    .collectionsAware(config.scope.isDefined || config.collections.nonEmpty)
    .scopeName(if (config.scope.isDefined && config.collections.isEmpty) config.scope.get else null)
    .collectionNames(
      config.collections
        .map(c => {
          val scope = config.scope match {
            case Some(s) => s
            case None    => "_default"
          }
          scope + "." + c
        })
        .asJava
    )
    .userAgent(Version.productName, Version.version, "stream")
    .credentials(conf.credentials.username, conf.credentials.password)
    .securityConfig(
      CouchbaseConnection(config.connectionIdentifier)
        .dcpSecurityConfig(conf, config.connectionIdentifier)
    )
    .bootstrapTimeout(DCPShared.bootstrapTimeout)
    .build()

  dcpClient.connect().block()

  override def initialOffset(): Offset = {
    val inputPartitions = config.numInputPartitions

    logInfo(
      s"Constructing initial offset for $inputPartitions input partitions " +
        s"over $numKvPartitions kv partitions (vbuckets)"
    )

    val groupedOffsets = reloadStartOffsetCheckpoints().zipWithIndex
      .groupBy(v => Math.floor(v._2 % inputPartitions))
      .values
      .map(v => {
        val offsets = v.toMap.keys.toMap
        KeyValuePartitionOffset(offsets, None).asInstanceOf[PartitionOffset]
      })
      .toList

    logDebug(s"Initial Offset is grouped into following partitions: $groupedOffsets")

    KeyValueOffset(groupedOffsets)
  }

  private def reloadStartOffsetCheckpoints() = {
    val startOffsets = mutable.Map[Int, KeyValueStreamOffset]() ++= (config.streamFrom match {
      case StreamFromVariants.FromBeginning =>
        (0 until numKvPartitions).map((_, KeyValueStreamOffset(StreamOffset.ZERO))).toMap
      case StreamFromVariants.FromNow =>
        currentOffsets()
    })

    val checkpointLog = new KeyValueSourceInitialOffsetWriter(sparkSession, checkpointLocation)

    checkpointLog.get(0) match {
      case Some(l) =>
        l.foreach(v => {
          startOffsets += (v._1 -> v._2)
        })
      case None =>
    }

    checkpointLog.add(0, startOffsets.toMap)
    startOffsets
  }

  def currentOffsets(): Map[Int, KeyValueStreamOffset] = {
    val partitionsAndSeqnos = dcpClient.getSeqnos.collectList().block().asScala

    val failoverLogs = dcpClient
      .failoverLogs(partitionsAndSeqnos.map(pas => Integer.valueOf(pas.partition())).asJava)
      .map[(Int, Long)](event => {
        val partition = DcpFailoverLogResponse.vbucket(event)
        val entries   = DcpFailoverLogResponse.entries(event)
        val vbUuid = if (entries.size() > 0) {
          entries.get(0).getUuid
        } else {
          0
        }
        (partition, vbUuid)
      })
      .collectList()
      .block()
      .asScala
      .toMap

    partitionsAndSeqnos
      .map(pas => {
        val vbUuid = failoverLogs(pas.partition())
        (pas.partition(), KeyValueStreamOffset(vbUuid, pas.seqno(), pas.seqno(), pas.seqno(), 0))
      })
      .toMap
  }

  /** Returns the number of KV partitions the connected cluster has. */
  def numKvPartitions: Int = dcpClient.numPartitions()

  override def deserializeOffset(json: String): Offset = {
    val offsets = Serialization
      .read[List[Map[String, Map[Int, KeyValueStreamOffset]]]](json)
      .map(so => KeyValuePartitionOffset(so("start"), so.get("end")))
    val o = KeyValueOffset(offsets)
    logTrace(s"Deserializing offset $json into $o")
    o
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    dcpClient.close()
  }

}

/** Holds the offset information for an individual vbucket.
  */
case class KeyValuePartitionOffset(
    streamStartOffsets: Map[Int, KeyValueStreamOffset],
    streamEndOffsets: Option[Map[Int, KeyValueStreamOffset]]
) extends PartitionOffset

case class KeyValueStreamOffset(
    vbuuid: Long,
    seqno: Long,
    snapshotStartSeqno: Long,
    snapshotEndSeqno: Long,
    collectionsManifestUid: Long
) {

  def toStreamOffset: StreamOffset = {
    new StreamOffset(
      vbuuid,
      seqno,
      new SnapshotMarker(snapshotStartSeqno, snapshotEndSeqno),
      collectionsManifestUid
    )
  }

}

object KeyValueStreamOffset {
  def apply(so: StreamOffset): KeyValueStreamOffset = {
    KeyValueStreamOffset(
      so.getVbuuid,
      so.getSeqno,
      so.getSnapshot.getStartSeqno,
      so.getSnapshot.getEndSeqno,
      so.getCollectionsManifestUid
    )
  }
}

case class KeyValueOffset(offsets: List[PartitionOffset]) extends Offset {
  override def json(): String = {
    implicit val formats = Serialization.formats(NoTypeHints)
    Serialization.write(offsets.map(o => {
      val po = o.asInstanceOf[KeyValuePartitionOffset]
      Map("start" -> po.streamStartOffsets, "end" -> po.streamEndOffsets)
    }))
  }
}

case class KeyValueInputPartition(
    schema: StructType,
    partitionOffset: KeyValuePartitionOffset,
    conf: CouchbaseConfig,
    config: KeyValueStreamConfig
) extends InputPartition
