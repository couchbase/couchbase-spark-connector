package com.couchbase.spark.sql.streaming

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.dcp.message.{DcpDeletionMessage, DcpMutationMessage, MessageUtil}
import com.couchbase.client.dcp.state.SessionState
import com.couchbase.client.dcp.{ControlEventHandler, DataEventHandler, StreamFrom, StreamTo}
import com.couchbase.client.deps.io.netty.buffer.ByteBuf
import com.couchbase.client.deps.io.netty.util.CharsetUtil
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark.Logging
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.sql.DefaultSource
import com.couchbase.spark.streaming.{Deletion, Mutation, StreamMessage}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.{CompositeOffset, Offset, Source}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import rx.lang.scala.Observable

import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.collection.mutable

class PartitionOffset(val vbid: Short, val seqno: Long) extends Offset {
  override def compareTo(other: Offset): Int = other match {
    case l: PartitionOffset => seqno.compareTo(l.seqno)
    case _ =>
      throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
  }


  override def toString = s"PartitionOffset($vbid, $seqno)"
}

class CouchbaseSource(sqlContext: SQLContext, user_schema: Option[StructType],
  parameters: Map[String, String])
  extends Source
    with Logging {

  val queues = new ConcurrentHashMap[Short, ConcurrentLinkedQueue[StreamMessage]](1024)

  val currentOffset = new AtomicReference[Offset]()

  val config = CouchbaseConfig(sqlContext.sparkContext.getConf)

  val client = CouchbaseConnection().streamClient(config)

  val used_schema = user_schema.getOrElse(CouchbaseSource.DEFAULT_SCHEMA)

  private val idFieldName = parameters.getOrElse("idField",
    DefaultSource.DEFAULT_DOCUMENT_ID_FIELD)

  initialize()

  private def initialize(): Unit = synchronized {
    client.controlEventHandler(new ControlEventHandler {
      override def onEvent(event: ByteBuf): Unit = {

      }
    })

    client.dataEventHandler(new DataEventHandler {
      override def onEvent(event: ByteBuf): Unit = {
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
          client.acknowledgeBuffer(event)
          deletion
        } else {
          client.acknowledgeBuffer(event)
          throw new IllegalStateException("Got unexpected DCP Data Event "
            + MessageUtil.humanize(event))
        }

        queues.get(vbucket.toShort).add(converted)
        event.release()
      }
    })

    client.connect().await()

    (0 until client.numPartitions()).foreach(p => {
      queues.put(p.toShort, new ConcurrentLinkedQueue[StreamMessage]())
    })

    client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).await()
    currentOffset.set(statesToOffsets(client.numPartitions().toShort, client.sessionState()))
    client.startStreaming().await()

    Observable
      .interval(2.seconds)
      .map(_ => statesToOffsets(client.numPartitions().toShort, client.sessionState()))
      .foreach(offset => currentOffset.set(offset))
  }

  def statesToOffsets(partitions: Short, ss: SessionState): CompositeOffset = {
    val offsets = (0 until partitions)
      .map(s => (s, ss.get(s)))
      .map(s => Some(new PartitionOffset(s._1.toShort, s._2.getStartSeqno)))
    new CompositeOffset(offsets)
  }

  override def schema: StructType = used_schema

  override def getOffset: Option[Offset] = Option(currentOffset.get())

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val results = mutable.ArrayBuffer[(Array[Byte], Array[Byte])]()

    val startOffset = start.map(_.asInstanceOf[CompositeOffset])
    val endOffset = end.asInstanceOf[CompositeOffset]

    endOffset.offsets.foreach(o => {
      if (o.isDefined) {
        val eo = o.get.asInstanceOf[PartitionOffset]
        val queue = queues.get(eo.vbid)

        var keepGoing = true
        while(keepGoing) {
          val item = queue.peek()
          if (item == null) {
            keepGoing = false
          } else {
            item match {
              case m: Mutation => {
                val so: Long = startOffset
                  .map(_.offsets.get(eo.vbid).get.asInstanceOf[PartitionOffset].seqno)
                  .getOrElse(0)
                if (m.bySeqno < so) {
                  queue.remove()
                  ack(m)
                } else if (m.bySeqno <= eo.seqno && m.bySeqno >= so) {
                  queue.remove()
                  ack(m)
                  results.append((m.key, m.content))
                } else {
                  keepGoing = false
                }
              }
              case _ => // todo: deletions are ignored right now.
            }
          }
        }
      }
    })

    val keyIdx = try {
      used_schema.fieldIndex(idFieldName)
    } catch {
      case iae: IllegalArgumentException => -1
    }

    if (used_schema == CouchbaseSource.DEFAULT_SCHEMA) {
      sqlContext.createDataFrame(results.map(t =>
        Row(new String(t._1, CharsetUtil.UTF_8), t._2)), used_schema)
    } else {
      sqlContext.read.schema(used_schema)
        .json(sqlContext.sparkContext.parallelize(
          results.map(t => {
            if (keyIdx >= 0) {
              JsonObject
                .fromJson(new String(t._2, CharsetUtil.UTF_8))
                .put(idFieldName, new String(t._1, CharsetUtil.UTF_8))
                .toString
            } else {
              new String(t._2, CharsetUtil.UTF_8)
            }
          })))
    }
  }

  def ack(m: Mutation): Unit = {
    client.acknowledgeBuffer(m.partition.toInt, m.ackBytes)
  }

  override def stop(): Unit = {
    client.disconnect().await()
  }

}

object CouchbaseSource {
  val DEFAULT_SCHEMA = StructType(
    StructField(DefaultSource.DEFAULT_DOCUMENT_ID_FIELD, StringType) ::
    StructField("value", BinaryType) :: Nil
  )
}
