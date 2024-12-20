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
    aggregations: Option[Aggregation],
    limit: Option[Int]
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
      case None => Array(QueryInputPartition(schema, filters, locations, aggregations, None, limit))
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

    val boundValueToString: BigDecimal => String = _.toString
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
    var currentValue = BigDecimal(lowerBound)
    val ans = new ArrayBuffer[QueryInputPartition]()
    while (i < numPartitions) {
      val lBoundValue = Math.max(currentValue.setScale(0, RoundingMode.HALF_UP).toLong, lowerBound)
      val lBoundFormatted = boundValueToString(lBoundValue)
      val lBound = if (i != 0) s"$column >= $lBoundFormatted" else null
      currentValue += preciseStride
      val uBoundValue = Math.min(currentValue.setScale(0, RoundingMode.HALF_UP).toLong, upperBound)
      val uBoundValueFormatted = boundValueToString(uBoundValue)
      val uBound = if (i != numPartitions - 1) s"$column < $uBoundValueFormatted" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound"
        } else {
          s"$lBound AND $uBound"
        }
      // Will apply the limit to each partition, but this is ok as Spark will also re-apply the limit at the global level
      ans += QueryInputPartition(schema, filters, locations, aggregations, Some(QueryPartitionBound(lBoundValue, uBoundValue, whereClause)), limit)
      i = i + 1
    }

    // Sanity check
    if (ans.length >= 2) {
      // Check upper and lower bounds
      require(ans.length == numPartitions, s"Partition count must match the number of partitions but did not for ${ans.length} ${numPartitions}")
      require(ans.head.bound.isDefined, s"Partition bounds must be defined but were not for ${ans.head}")
      require(ans.last.bound.isDefined, s"Partition bounds must be defined but were not for ${ans.last}")
      require(ans.head.bound.get.lowerBoundInclusive == lowerBound, s"Partition bounds must match the lower bound but did not for ${ans.head}")
      require(ans.last.bound.get.upperBoundExclusive == upperBound, s"Partition bounds must match the upper bound but did not for ${ans.last}")

      // Check middle queries
      for (i <- 0 until ans.length - 1) {
        val ans1 = ans(i)
        val ans2 = ans(i + 1)
        require(ans1.bound.isDefined && ans2.bound.isDefined, s"Partition bounds must be defined but were not for ${ans1} ${ans2}")
        require(ans1.bound.get.upperBoundExclusive == ans2.bound.get.lowerBoundInclusive, s"Partition bounds must be contiguous but were not for ${ans1} ${ans2}")
      }
    }

    ans.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new QueryPartitionReaderFactory(conf, readConfig)
}
