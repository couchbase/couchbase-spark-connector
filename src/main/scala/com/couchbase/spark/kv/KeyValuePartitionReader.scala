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

import com.couchbase.client.dcp.highlevel._
import com.couchbase.client.dcp.message.{DcpFailoverLogResponse, StreamEndReason}
import com.couchbase.client.dcp.state.PartitionState
import com.couchbase.client.dcp.{Client, StreamTo}
import com.couchbase.spark.config.CouchbaseConnection
import com.couchbase.spark.util.Version
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReader, PartitionOffset}
import org.apache.spark.unsafe.types.UTF8String

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

/**
 * The KeyValuePartitionReader is responsible for actually streaming the mutations from a number of vbuckets.
 *
 * @param partition the partition that contains info about what to stream.
 * @param continuous if this is a continuous stream or microbatch.
 */
class KeyValuePartitionReader(partition: KeyValueInputPartition, continuous: Boolean)
  extends ContinuousPartitionReader[InternalRow] {

  private val streamConfig = partition.config
  private val couchbaseConfig = partition.conf
  private val currentStreamOffsets = new ConcurrentHashMap[Int, KeyValueStreamOffset](partition.partitionOffset.streamStartOffsets.asJava)
  private val streamsCompleted = new ConcurrentHashMap[Int, StreamEndReason]()
  private val changeQueue = new LinkedBlockingQueue[DocumentChange](100)
  private val errorQueue = new LinkedBlockingQueue[Throwable](1)
  private var currentEntry: DocumentChange = _
  private val ownedVbuckets = partition.partitionOffset.streamStartOffsets.keys

  private val dcpClient = Client
    .builder()
    .seedNodes(CouchbaseConnection().dcpSeedNodes(couchbaseConfig))
    .securityConfig(CouchbaseConnection().dcpSecurityConfig(couchbaseConfig))
    .flowControl(streamConfig.flowControlBufferSize.getOrElse(1024 * 1024 * 10))
    .mitigateRollbacks(Duration(streamConfig.persistencePollingInterval.getOrElse("100ms")).toMicros, TimeUnit.MICROSECONDS)
    .userAgent(Version.productName, Version.version, "reader")
    .credentials(couchbaseConfig.credentials.username, couchbaseConfig.credentials.password)
    .bucket(streamConfig.bucket)
    .collectionsAware(streamConfig.scope.isDefined || streamConfig.collections.nonEmpty)
    .scopeName(if (streamConfig.scope.isDefined && streamConfig.collections.isEmpty) streamConfig.scope.get else null)
    .collectionNames(streamConfig.collections.map(c => {
      val scope = streamConfig.scope match {
        case Some(s) => s
        case None => "_default"
      }
      scope + "." + c
    }).asJava)
    .noValue(!streamConfig.streamContent)
    .xattrs(streamConfig.streamXattrs)
    .build()

  attachAndConnectDcpListener()
  initAndStartDcpStream()

  /**
   * Attaches the change and error queues to the high-level DCP API.
   *
   * After the listener is attached, the DCP client is instructed to connect.
   */
  private def attachAndConnectDcpListener(): Unit = {
    dcpClient.listener(new DatabaseChangeListener {
      override def onMutation(mutation: Mutation): Unit = changeQueue.put(mutation)

      override def onDeletion(deletion: Deletion): Unit = changeQueue.put(deletion)

      override def onFailure(streamFailure: StreamFailure): Unit = errorQueue.offer(streamFailure.getCause)

      override def onStreamEnd(streamEnd: StreamEnd): Unit = {
        streamsCompleted.put(streamEnd.getVbucket, streamEnd.getReason)
      }
    }, FlowControlMode.AUTOMATIC)

    dcpClient.connect().block()
  }

  /**
   * Initializes the DCP stream state and starts vbucket streaming.
   *
   * The code first initializes the state for all vbuckets, since that also loads the current failover logs. Then it
   * applies the start (and optional end) sequence numbers for each partition which are then subsequently started. If
   * no explicit end offset is defined, -1 will be used implicitly (which indicates no end).
   */
  private def initAndStartDcpStream(): Unit = {
    val partitionOffset = partition.partitionOffset
    val startOffsets = partitionOffset.streamStartOffsets
    val vbIds = ownedVbuckets.map(Integer.valueOf).toSeq.sorted.asJava

    dcpClient.initializeState(streamConfig.streamFrom.asDcpStreamFrom, StreamTo.INFINITY).block()

    if (streamConfig.streamFrom.fromBeginning()) {
      // The DCP client does not initialize the failover log when starting from the beginning, so do that.
      dcpClient.failoverLogs(vbIds).doOnNext(event => {
        val partition = DcpFailoverLogResponse.vbucket(event)
        val ps = dcpClient.sessionState().get(partition)
        ps.setFailoverLog(DcpFailoverLogResponse.entries(event))
        dcpClient.sessionState().set(partition, ps)
      }).blockLast()
    }

    startOffsets.foreach(startOffset => {
      val vbId = startOffset._1
      val partitionState = PartitionState.fromOffset(startOffset._2.toStreamOffset)

      partitionState.setFailoverLog(dcpClient.sessionState().get(vbId).getFailoverLog)
      partitionOffset.streamEndOffsets.foreach(eo => partitionState.setEndSeqno(eo(vbId).seqno))

      dcpClient.sessionState().set(vbId, partitionState)
    })

    dcpClient.startStreaming(vbIds).block()
  }

  override def getOffset: PartitionOffset = KeyValuePartitionOffset(currentStreamOffsets.asScala.toMap, None)

  override def next(): Boolean = next(0)

  @tailrec
  private def next(numRetries: Int): Boolean = {
    checkErrorQueue()
    if (continuous) {
      currentEntry = changeQueue.take()
      currentStreamOffsets.put(currentEntry.getVbucket, KeyValueStreamOffset.apply(currentEntry.getOffset))
      true
    } else {
      currentEntry = changeQueue.poll()

      if (currentEntry != null) {
        currentStreamOffsets.put(currentEntry.getVbucket, KeyValueStreamOffset.apply(currentEntry.getOffset))
        true
      } else if (batchStreamComplete) {
        false
      } else {
        if (numRetries > 0) {
          // We need to wait for more data in the queue, but to not run into a stack overflow for
          // now just keep sleeping for longer and longer on every retry (with a max of 10ms)
          Thread.sleep(numRetries.min(10))
        }
        next(numRetries + 1)
      }
    }
  }

  /**
   * Checks if the current batch to stream is complete.
   *
   * @return true if it is complete (all owned vbuckets are complete).
   */
  private def batchStreamComplete: Boolean = {
    streamsCompleted.size() >= ownedVbuckets.size
  }

  private def checkErrorQueue(): Unit = {
    val error = errorQueue.poll()
    if (error != null) {
      throw new RuntimeException(error)
    }
  }

  override def get(): InternalRow = {
    InternalRow.fromSeq(partition.schema.fieldNames.map {
      case "id" => UTF8String.fromString(currentEntry.getKey)
      case "content" => currentEntry.getContent
      case "deletion" => !currentEntry.isMutation
      case "cas" => currentEntry.getCas
      case "scope" => UTF8String.fromString(currentEntry.getCollection.scope().name())
      case "collection" => UTF8String.fromString(currentEntry.getCollection.name())
      case "timestamp" => ChronoUnit.MICROS.between(Instant.EPOCH, currentEntry.getTimestamp)
      case "vbucket" => currentEntry.getVbucket
      case "xattrs" => ArrayBasedMapData(currentEntry.getXattrs, k => k.toString, v => v.toString)
      case name => throw new UnsupportedOperationException("Unsupported field name in schema " + name)
    }.toSeq)
  }

  override def close(): Unit = dcpClient.close()

}
