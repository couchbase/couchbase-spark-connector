/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.spark.columnar

import com.couchbase.spark.config.CouchbaseConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ColumnarScan(
                     schema: StructType,
                     readConfig: ColumnarReadConfig,
                     filters: Array[Filter],
                     aggregations: Option[Aggregation],
                     limit: Option[Int]
) extends Scan {

  private lazy val conf =
    CouchbaseConfig(SparkSession.active.sparkContext.getConf, readConfig.connectionIdentifier)

  override def readSchema(): StructType = schema
  override def toBatch: Batch = new ColumnarBatchRead(schema, conf, readConfig, filters, aggregations, limit)

}
