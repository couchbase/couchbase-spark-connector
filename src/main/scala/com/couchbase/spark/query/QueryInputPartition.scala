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

import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class QueryInputPartition(
    val schema: StructType,
    val filters: Array[Filter],
    val locations: Array[String],
    val aggregations: Option[Aggregation],
    val bound: Option[QueryPartitionBound]
) extends InputPartition {
  override def preferredLocations(): Array[String] = locations
}

case class QueryPartitionBound(whereClause: String)

case class PartitioningConfig(partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Long)
