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

package com.couchbase.spark.query

import com.couchbase.client.core.service.ServiceType
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

class QueryBatch(
    schema: StructType,
    conf: CouchbaseConfig,
    readConfig: QueryReadConfig,
    filters: Array[Filter],
    aggregations: Option[Aggregation]
) extends Batch with Logging {

  override def planInputPartitions(): Array[InputPartition] = {
    val conn = CouchbaseConnection(readConfig.connectionIdentifier)
    val core = conn.cluster(conf).async.core
    val config = core.clusterConfig()

    val locations: Array[String] = if (config.globalConfig() != null) {
      config
        .globalConfig()
        .portInfos()
        .asScala
        .filter(p => p.ports().containsKey(ServiceType.QUERY))
        .map(p => {
          val aa = core.context().alternateAddress()
          if (aa != null && aa.isPresent) {
            p.alternateAddresses().get(aa.get()).hostname()
          } else {
            p.hostname()
          }
        })
        .toArray
    } else {
      Array()
    }

    val partitions: Array[QueryInputPartition] = readConfig.partition match {
      case Some(value) => makePartitions(value, locations)
      case None => Array(QueryInputPartition(schema, filters, locations, aggregations, None))
    }

    partitions.map(_.asInstanceOf[InputPartition])
  }

  private def makePartitions(partitioning: PartitioningConfig, locations: Array[String]): Array[QueryInputPartition] = {
    // This code owes a debt to
    // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala
    //    val partitionColumn = partitioning.partitionColumn
    //    val lowerBound = partitioning.lowerBound
    //    val upperBound = partitioning.upperBound
    // In this initial release, the partition column must be numeric (or at least, support standard comparison operators)

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require(lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val boundValueToString: Long => String = _.toString
    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions || /* check for overflow */
        (upperBound - lowerBound) < 0) {
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; " +
          s"Lower bound: ${boundValueToString(lowerBound)}; " +
          s"Upper bound: ${boundValueToString(upperBound)}.")
        upperBound - lowerBound
      }

    // Overflow can happen if you subtract then divide. For example:
    // (Long.MaxValue - Long.MinValue) / (numPartitions - 2).
    // Also, using fixed-point decimals here to avoid possible inaccuracy from floating point.
    val upperStride = (upperBound / BigDecimal(numPartitions))
      .setScale(18, RoundingMode.HALF_EVEN)
    val lowerStride = (lowerBound / BigDecimal(numPartitions))
      .setScale(18, RoundingMode.HALF_EVEN)

    val preciseStride = upperStride - lowerStride
    val stride = preciseStride.toLong

    // Determine the number of strides the last partition will fall short of compared to the
    // supplied upper bound. Take half of those strides, and then add them to the lower bound
    // for better distribution of the first and last partitions.
    val lostNumOfStrides = (preciseStride - stride) * numPartitions / stride
    val lowerBoundWithStrideAlignment = lowerBound +
      ((lostNumOfStrides / 2) * stride).setScale(0, RoundingMode.HALF_UP).toLong

    var i: Int = 0
    val column = partitioning.partitionColumn
    var currentValue = lowerBoundWithStrideAlignment
    val ans = new ArrayBuffer[QueryInputPartition]()
    while (i < numPartitions) {
      val lBoundValue = boundValueToString(currentValue)
      val lBound = if (i != 0) s"$column >= $lBoundValue" else null
      currentValue += stride
      val uBoundValue = boundValueToString(currentValue)
      val uBound = if (i != numPartitions - 1) s"$column < $uBoundValue" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound"
        } else {
          s"$lBound AND $uBound"
        }
      ans += QueryInputPartition(schema, filters, locations, aggregations, Some(QueryPartitionBound(whereClause)))
      i = i + 1
    }
    ans.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new QueryPartitionReaderFactory(conf, readConfig)
}
