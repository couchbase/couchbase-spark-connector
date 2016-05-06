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
package com.couchbase.spark.streaming

import com.couchbase.client.core.message.dcp._
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.streaming.state.NoopStateSerializer
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import rx.lang.scala.JavaConversions._

import scala.collection.mutable.ArrayBuffer

abstract class StreamMessage
case class Snapshot(seqStart: Long, seqEnd: Long, memory: Boolean, disk: Boolean,
                    checkpoint: Boolean, ack: Boolean) extends StreamMessage
case class Mutation(key: String, content: Array[Byte], expiry: Integer, cas: Long,
                    flags: Int, lockTime: Int) extends StreamMessage
case class Deletion(key: String, cas: Long) extends StreamMessage

class CouchbaseInputDStream
  (@transient ssc: StreamingContext, storageLevel: StorageLevel, bucket: String = null,
   from: StreamFrom = FromNow, to: StreamTo = ToInfinity)
  extends ReceiverInputDStream[StreamMessage](ssc)
  with Logging {

  private val cbConfig = CouchbaseConfig(ssc.sparkContext.getConf)
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
    logInfo(s"Starting Couchbase (DCP) Stream against Bucket $bucketName")

    val bucket = CouchbaseConnection().bucket(config, bucketName).async()
    val core = bucket.core().toBlocking.single()

    val reader = new CouchbaseReader(bucketName, core, bucket.environment(),
      new NoopStateSerializer())

    reader.connect()

    val streamFrom = from match {
      case FromBeginning => reader.startState()
      case FromNow => reader.currentState()
      case _ => throw new UnsupportedOperationException("This Streaming Start is not supported.")
    }

    val streamTo = to match {
      case ToInfinity=> reader.endState()
      case _ => throw new UnsupportedOperationException("This Streaming End is not supported.")
    }

    toScalaObservable(reader.run(streamFrom, streamTo))
        .foreach(req => {
          val converted = req match {
            case msg: SnapshotMarkerMessage =>
              new Snapshot(msg.startSequenceNumber(), msg.endSequenceNumber(), msg.memory(),
                msg.disk(), msg.checkpoint(), msg.ack())
            case msg: MutationMessage =>

              val data = new Array[Byte](msg.content().readableBytes())
              msg.content().readBytes(data)
              val mutation = new Mutation(msg.key(), data, msg.expiration(), msg.cas(),
                msg.flags(), msg.lockTime())
              mutation
            case msg: RemoveMessage =>
              new Deletion(msg.key(), msg.cas())
            case _ =>
              logError("Unknown DCP Stream Message $msg")
              null
          }
          store(ArrayBuffer[StreamMessage](converted))
          reader.consumed(req.asInstanceOf[DCPMessage])
        })
  }

  override def onStop(): Unit = {
    logInfo(s"Stopping Couchbase (DCP) Stream against Bucket $bucketName")
  }

}
