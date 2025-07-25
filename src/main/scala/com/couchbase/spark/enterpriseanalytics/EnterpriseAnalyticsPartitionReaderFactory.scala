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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class EnterpriseAnalyticsPartitionReaderFactory(
    conf: EnterpriseAnalyticsConfig,
    readConfig: EnterpriseAnalyticsReadConfig
) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val part = partition.asInstanceOf[EnterpriseAnalyticsInputPartition]
    new EnterpriseAnalyticsPartitionReader(
      part.schema,
      conf,
      readConfig,
      part.filters,
      part.aggregations,
      part.limit
    )
  }
}
