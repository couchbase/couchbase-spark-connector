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

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReaderFactory, ContinuousStream, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType

class KeyValueContinuousStream(schema: StructType, config: KeyValueStreamConfig, checkpointLocation: String)
  extends KeyValueDataStream(config, checkpointLocation)
    with ContinuousStream {

  override def planInputPartitions(start: Offset): Array[InputPartition] = {
    val inputPartitions = config.numInputPartitions
    val keyValueOffset = start.asInstanceOf[KeyValueOffset]
    val streamOffsets = keyValueOffset.offsets.flatMap(po => po.asInstanceOf[KeyValuePartitionOffset].streamStartOffsets)

    logInfo(s"(Re)planning $inputPartitions input partitions " +
      s"over $numKvPartitions kv partitions (vbuckets)")

    val groupedOffsets = streamOffsets
      .zipWithIndex
      .groupBy(v => Math.floor(v._2 % inputPartitions))
      .values
      .map(v =>  {
        val startOffsets = v.map(x => x._1).toMap
        KeyValueInputPartition(
          schema, KeyValuePartitionOffset(startOffsets, None), conf, config
        ).asInstanceOf[InputPartition]
      })
      .toArray

    logDebug(s"Offset is grouped into following input partitions: $groupedOffsets")

    groupedOffsets
  }

  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory = {
    (partition: InputPartition) => {
      new KeyValuePartitionReader(partition.asInstanceOf[KeyValueInputPartition], true)
    }
  }

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = KeyValueOffset(offsets.toList)

}