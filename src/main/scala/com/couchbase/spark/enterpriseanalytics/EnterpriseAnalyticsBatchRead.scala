/*
 * Copyright (c) 2025 Couchbase, Inc.
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

package com.couchbase.spark.enterpriseanalytics

import com.couchbase.spark.enterpriseanalytics.config.EnterpriseAnalyticsConfig
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class EnterpriseAnalyticsBatchRead(
    schema: StructType,
    conf: EnterpriseAnalyticsConfig,
    readConfig: EnterpriseAnalyticsReadConfig,
    filters: Array[Filter],
    aggregations: Option[Aggregation],
    limit: Option[Int]
) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new EnterpriseAnalyticsInputPartition(schema, filters, aggregations, limit))
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new EnterpriseAnalyticsPartitionReaderFactory(conf, readConfig)
}
