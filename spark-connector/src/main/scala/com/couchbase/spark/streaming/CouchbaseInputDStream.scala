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
package com.couchbase.spark.streaming

import com.couchbase.client.dcp._
import com.couchbase.client.dcp.message._
import com.couchbase.client.dcp.transport.netty.ChannelFlowController
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf
import com.couchbase.spark.Logging
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import rx.{CompletableSubscriber, Subscription}

abstract class StreamMessage
case class Mutation(key: Array[Byte], content: Array[Byte], expiry: Integer, cas: Long,
                    partition: Short, flags: Int, lockTime: Int, bySeqno: Long,
                    revisionSeqno: Long, ackBytes: Int) extends StreamMessage
case class Deletion(key: Array[Byte], cas: Long, partition: Short, bySeqno: Long,
                    revisionSeqno: Long, ackBytes: Int) extends StreamMessage

class CouchbaseInputDStream
  (_ssc: StreamingContext, storageLevel: StorageLevel, bucket: String = null,
   from: StreamFrom = FromNow, to: StreamTo = ToInfinity)
  extends ReceiverInputDStream[StreamMessage](_ssc) {

  private val cbConfig = CouchbaseConfig(_ssc.sparkContext.getConf)
  private val bucketName = Option(bucket).getOrElse(cbConfig.buckets.head.name)

  override def getReceiver(): Receiver[StreamMessage] = {
    new CouchbaseReceiver(cbConfig, bucketName, storageLevel, from, to)
  }

}

class CouchbaseReceiver(config: CouchbaseConfig, bucketName: String, storageLevel: StorageLevel,
  from: StreamFrom, to: StreamTo)
  extends Receiver[StreamMessage](storageLevel)
  with Logging {

  override def onStart(): Unit = {
    val client = CouchbaseConnection().streamClient(config, bucketName)

    // Attach Callbacks
    client.controlEventHandler(new ControlEventHandler {
      override def onEvent(flowController: ChannelFlowController, event: ByteBuf): Unit = {
        if (RollbackMessage.is(event)) {
          val partition = RollbackMessage.vbucket(event)
          client.rollbackAndRestartStream(partition, RollbackMessage.seqno(event))
            .subscribe(new CompletableSubscriber {
              override def onCompleted(): Unit = {
                logTrace("DCP Rollback completed")
              }

              override def onError(e: Throwable): Unit = {
                logWarning("Error during DCP Rollback!", e)
              }

              override def onSubscribe(d: Subscription): Unit = {}
            })
        } else if (DcpSnapshotMarkerRequest.is(event)) {
          flowController.ack(event)
        } else {
          event.release()
          throw new IllegalStateException("Got unexpected DCP Control Event "
            + MessageUtil.humanize(event))
        }
        event.release()
      }
    })

    client.dataEventHandler(new DataEventHandler {
      override def onEvent(flowController: ChannelFlowController, event: ByteBuf): Unit = {
        val converted: StreamMessage = if (DcpMutationMessage.is(event)) {
          val data = new Array[Byte](DcpMutationMessage.content(event).readableBytes())
          DcpMutationMessage.content(event).readBytes(data)
          val key = new Array[Byte](DcpMutationMessage.key(event).readableBytes())
          DcpMutationMessage.key(event).readBytes(key)
          Mutation(
            key,
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
          Deletion(
            key,
            DcpDeletionMessage.cas(event),
            DcpDeletionMessage.partition(event),
            DcpDeletionMessage.bySeqno(event),
            DcpDeletionMessage.revisionSeqno(event),
            event.readableBytes()
          )
        } else {
          event.release()
          throw new IllegalStateException("Got unexpected DCP Data Event "
            + MessageUtil.humanize(event))
        }

        store(converted)
        flowController.ack(event)
        event.release()
      }
    })

    // Connect to the nodes
    client.connect().await()

    // Initialize the state as desired
    if (from == FromNow && to == ToInfinity) {
      client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await()
    } else if (from == FromBeginning && to == ToInfinity) {
      client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).await()
    } else if (from == FromBeginning && to == ToNow) {
      client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).await()
    } else {
      throw new IllegalArgumentException("Unsupported From/To Combination!")
    }

    // Start streaming for partitions
    client.startStreaming().await()
  }

  override def onStop(): Unit = {
    val client = CouchbaseConnection().streamClient(config, bucketName)
    client.stopStreaming().await()
    client.disconnect().await()
  }

}
