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

package com.couchbase.spark.analytics

import com.couchbase.spark.config.CouchbaseConfig
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class AnalyticsBatch(schema: StructType, conf: CouchbaseConfig, readConfig: AnalyticsReadConfig, filters: Array[Filter]) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = Array(new AnalyticsInputPartition(schema, filters))

  override def createReaderFactory(): PartitionReaderFactory = new AnalyticsPartitionReaderFactory(conf, readConfig)
}
