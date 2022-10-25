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

import com.couchbase.spark.DefaultConstants
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class QueryTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    readConfig: QueryReadConfig
) extends SupportsRead {

  override def name(): String = {
    if (readConfig.scope.isEmpty && readConfig.collection.isEmpty) {
      readConfig.bucket
    } else {
      readConfig.bucket + ":" +
        readConfig.scope.getOrElse(DefaultConstants.DefaultScopeName) + ":" +
        readConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)
    }
  }
  override def schema(): StructType                   = schema
  override def partitioning(): Array[Transform]       = partitioning
  override def properties(): util.Map[String, String] = properties
  override def capabilities(): util.Set[TableCapability] =
    Set[TableCapability](
      TableCapability.BATCH_READ,
      TableCapability.V1_BATCH_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA
    ).asJava
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new QueryScanBuilder(schema, readConfig)

}
