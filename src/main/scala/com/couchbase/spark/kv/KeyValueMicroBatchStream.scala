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

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types.StructType

class KeyValueMicroBatchStream(
    schema: StructType,
    config: KeyValueStreamConfig,
    checkpointLocation: String
) extends KeyValueDataStream(config, checkpointLocation)
    with MicroBatchStream {

  override def latestOffset(): Offset = {
    val co = currentOffsets()
    KeyValueOffset(List(KeyValuePartitionOffset(Map(), Some(co))))
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val inputPartitions     = config.numInputPartitions
    val keyValueStartOffset = start.asInstanceOf[KeyValueOffset]
    val keyValueEndOffset   = end.asInstanceOf[KeyValueOffset]
    val streamStartOffsets = keyValueStartOffset.offsets.flatMap(po => {
      val kvPo = po.asInstanceOf[KeyValuePartitionOffset]
      // If the end is present we are 1+ iterations in, only the first ever actually uses the start
      // offset (since the last end is the new start).
      kvPo.streamEndOffsets.getOrElse(kvPo.streamStartOffsets)
    })
    val streamEndOffsets = keyValueEndOffset.offsets
      .flatMap(po => po.asInstanceOf[KeyValuePartitionOffset].streamEndOffsets.get)

    logInfo(
      s"(Re)planning $inputPartitions input partitions " +
        s"over $numKvPartitions kv partitions (vbuckets)"
    )

    val groupedOffsets = streamStartOffsets.zipWithIndex
      .groupBy(v => Math.floor(v._2 % inputPartitions))
      .values
      .map(v => {
        val startOffsets = v.map(x => x._1).toMap
        val endOffsets = startOffsets.keys
          .map(vbid => {
            val so = streamEndOffsets.find(v => v._1 == vbid).get
            so
          })
          .toMap

        KeyValueInputPartition(
          schema,
          KeyValuePartitionOffset(startOffsets, Some(endOffsets)),
          conf,
          config
        ).asInstanceOf[InputPartition]
      })
      .toArray

    logDebug(s"Offset is grouped into following input partitions: $groupedOffsets")

    groupedOffsets
  }

  override def createReaderFactory(): PartitionReaderFactory = { (partition: InputPartition) =>
    {
      new KeyValuePartitionReader(partition.asInstanceOf[KeyValueInputPartition], false)
    }
  }

}
