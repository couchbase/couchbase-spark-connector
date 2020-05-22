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
package com.couchbase.spark.sql.streaming

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.dcp.message.{DcpDeletionMessage, DcpMutationMessage, DcpSnapshotMarkerRequest, MessageUtil}
import com.couchbase.client.dcp._
import com.couchbase.client.dcp.state.SessionState
import com.couchbase.client.dcp.transport.netty.ChannelFlowController
import com.couchbase.client.deps.io.netty.buffer.ByteBuf
import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark.Logging
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.sql.DefaultSource
import com.couchbase.spark.streaming.{Deletion, Mutation, StreamMessage}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import rx.lang.scala.Observable

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._

class CouchbaseSource(sqlContext: SQLContext, userSchema: Option[StructType],
  parameters: Map[String, String])
  extends Source
    with Logging {

  val queues = new ConcurrentHashMap[Short, ConcurrentLinkedQueue[StreamMessage]](1024)
  val flowControllers = new ConcurrentHashMap[Short, ChannelFlowController](1024)
  val currentOffset = new AtomicReference[Offset]()
  val config = CouchbaseConfig(sqlContext.sparkContext.getConf)
  val client: Client = CouchbaseConnection().streamClient(config)
  val usedSchema: StructType = userSchema.getOrElse(CouchbaseSource.DEFAULT_SCHEMA)

  private val idFieldName = parameters.getOrElse("idField",
    DefaultSource.DEFAULT_DOCUMENT_ID_FIELD)

  private val streamFrom = parameters.getOrElse("streamFrom",
    DefaultSource.DEFAULT_STREAM_FROM)

  private val streamTo = parameters.getOrElse("streamTo",
    DefaultSource.DEFAULT_STREAM_TO)

  initialize()

  private def initialize(): Unit = synchronized {
    client.controlEventHandler(new ControlEventHandler {
      override def onEvent(flowController: ChannelFlowController, event: ByteBuf): Unit = {
        if (DcpSnapshotMarkerRequest.is(event)) {
          flowController.ack(event)
        }
        event.release
      }
    })

    client.dataEventHandler(new DataEventHandler {
      override def onEvent(flowController: ChannelFlowController, event: ByteBuf): Unit = {
        var vbucket = -1
        val converted: StreamMessage = if (DcpMutationMessage.is(event)) {
          val data = new Array[Byte](DcpMutationMessage.content(event).readableBytes())
          DcpMutationMessage.content(event).readBytes(data)
          val key = new Array[Byte](DcpMutationMessage.key(event).readableBytes())
          DcpMutationMessage.key(event).readBytes(key)
          vbucket = DcpMutationMessage.partition(event)
          Mutation(key,
            data,
            DcpMutationMessage.expiry(event),
            DcpMutationMessage.cas(event),
            DcpMutationMessage.partition(event),
            DcpMutationMessage.flags(event),
            DcpMutationMessage.lockTime(event),
            DcpDeletionMessage.bySeqno(event),
            DcpDeletionMessage.revisionSeqno(event),
            event.readableBytes()
          )
        } else if (DcpDeletionMessage.is(event)) {
          val key = new Array[Byte](DcpDeletionMessage.key(event).readableBytes())
          DcpDeletionMessage.key(event).readBytes(key)
          vbucket = DcpMutationMessage.partition(event)
          val deletion = Deletion(
            key,
            DcpDeletionMessage.cas(event),
            DcpDeletionMessage.partition(event),
            DcpDeletionMessage.bySeqno(event),
            DcpDeletionMessage.revisionSeqno(event),
            event.readableBytes()
          )
          flowController.ack(event)
          deletion
        } else {
          flowController.ack(event)
          throw new IllegalStateException("Got unexpected DCP Data Event "
            + MessageUtil.humanize(event))
        }

        if (!flowControllers.containsKey(vbucket)) {
          flowControllers.put(vbucket.toShort, flowController)
        }
        queues.get(vbucket.toShort).add(converted)
        event.release()
      }
    })

    client.connect().await()

    (0 until client.numPartitions).foreach(p => {
      queues.put(p.toShort, new ConcurrentLinkedQueue[StreamMessage]())
    })

    val from = streamFrom.toLowerCase() match {
      case "beginning" => StreamFrom.BEGINNING
      case "now" => StreamFrom.NOW
      case _ => throw new IllegalArgumentException("Could not match StreamFrom " + streamFrom)
    }

    val to = streamTo.toLowerCase() match {
      case "infinity" => StreamTo.INFINITY
      case "now" => StreamTo.NOW
      case _ => throw new IllegalArgumentException("Could not match StreamTo " + streamFrom)
    }

    logInfo(s"Starting Couchbase Structured Stream from $from to $to")
    client.initializeState(from, to).await()
    currentOffset.set(statesToOffsets)
    client.startStreaming().await()

    Observable
      .interval(2.seconds)
      .map(_ => statesToOffsets)
      .foreach(offset => currentOffset.set(offset))

  }

  private def statesToOffsets: Offset = {
    val ss: SessionState = client.sessionState()
    val offsets = (0 until client.numPartitions)
      .map(s => (s.toShort, ss.get(s).getStartSeqno))
    CouchbaseSourceOffset(offsets.toMap)
  }

  override def schema: StructType = usedSchema

  override def getOffset: Option[Offset] = Option(currentOffset.get())

  /**
    * Returns the data between the given start and end Offset.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"GetBatch called with start = $start, end = $end")

    val startOffset = start.map(CouchbaseSourceOffset.convertToCouchbaseSourceOffset(_))
    val endOffset = CouchbaseSourceOffset.convertToCouchbaseSourceOffset(end)

    val results = mutable.ArrayBuffer[(Array[Byte], Array[Byte])]()

    endOffset.partitionToOffsets.foreach(o => {
      val partition: Short = o._1
      val endSeqno: Long = o._2
      val queue = queues.get(partition)

      var keepGoing = true
      while(keepGoing) {
        val item = queue.peek
        if (item == null) {
          keepGoing = false
        } else item match {
          case m: Mutation =>
            val startSeqno: Long = startOffset.map(o => o.partitionToOffsets(partition))
              .getOrElse(0)
            if (m.bySeqno < startSeqno) {
              queue.remove()
              ack(m)
            } else if (m.bySeqno <= endSeqno && m.bySeqno >= startSeqno) {
              queue.remove()
              ack(m)
              results.append((m.key, m.content))
            } else {
              keepGoing = false
            }
          case _ => // deletions are ignored for now
        }
      }
    })

    val keyIdx = try {
      usedSchema.fieldIndex(idFieldName)
    } catch {
      case _: IllegalArgumentException => -1
    }

    if (usedSchema == CouchbaseSource.DEFAULT_SCHEMA) {
      val rows = results.map(t => Row(new String(t._1, CharsetUtil.UTF_8), t._2))
      val rdd = sqlContext.sparkContext.parallelize(rows)
      DataFrameCreation.createStreamingDataFrame(sqlContext, rdd, usedSchema)
    } else {

      val rdd: RDD[String] = sqlContext.sparkContext.parallelize(results.map(t => {
        if (keyIdx >= 0) {
          JsonObject
            .fromJson(new String(t._2, CharsetUtil.UTF_8))
            .put(idFieldName, new String(t._1, CharsetUtil.UTF_8))
            .toString
        } else {
          new String(t._2, CharsetUtil.UTF_8)
        }
      }))

      val dataset: Dataset[String] = sqlContext.sparkSession.createDataset(rdd)(Encoders.STRING)
      val jsonDf: DataFrame = sqlContext.read.schema(usedSchema).json(dataset)

      DataFrameCreation.createStreamingDataFrame(sqlContext, jsonDf, usedSchema)
    }
  }

  private def ack(m: Mutation): Unit = {
    flowControllers.get(m.partition).ack(m.ackBytes)
  }

  override def stop(): Unit = {
    client.disconnect().await()
  }

}

object CouchbaseSource {

  def DEFAULT_SCHEMA: StructType = StructType(Seq(
    StructField(DefaultSource.DEFAULT_DOCUMENT_ID_FIELD, StringType),
    StructField("value", BinaryType)
  ))

}