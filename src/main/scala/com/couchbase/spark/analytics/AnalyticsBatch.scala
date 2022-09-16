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

import com.couchbase.client.core.service.ServiceType
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection, CouchbaseConnectionPool}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import collection.JavaConverters._

class AnalyticsBatch(schema: StructType, conf: CouchbaseConfig, filters: Array[Filter], aggregations: Option[Aggregation]) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    val core = CouchbaseConnectionPool().getConnection(conf).cluster().async.core
    val config = core.clusterConfig()

    val locations = if (config.globalConfig() != null) {
      config
        .globalConfig()
        .portInfos()
        .asScala
        .filter(p => p.ports().containsKey(ServiceType.ANALYTICS))
        .map(p => {
          val aa = core.context().alternateAddress()
          if (aa != null && aa.isPresent) {
            p.alternateAddresses().get(aa.get()).hostname()
          } else {
            p.hostname()
          }
        }).toArray
    } else {
      Array[String]()
    }

    Array(new AnalyticsInputPartition(schema, filters, locations, aggregations))
  }

  override def createReaderFactory(): PartitionReaderFactory = new AnalyticsPartitionReaderFactory(conf)
}
