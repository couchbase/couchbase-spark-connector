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

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class ColumnarTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    readConfig: ColumnarReadConfig,
    writeConfig: ColumnarWriteConfig
) extends SupportsRead
    // Write support is temporarily removed due to server limitations
    //  with SupportsWrite
    {
  override def name(): String = {
    s"${readConfig.database}.${readConfig.scope}.${readConfig.collection}"
  }
  override def schema(): StructType                   = schema
  override def partitioning(): Array[Transform]       = partitioning
  override def properties(): util.Map[String, String] = properties
  override def capabilities(): util.Set[TableCapability] =
    Set[TableCapability](
      TableCapability.BATCH_READ
        // TableCapability.BATCH_WRITE,
    ).asJava
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ColumnarScanBuilder(schema, readConfig)

      // Writing has been temporarily removed until server support increases.
      // Check in history for previously existing files including ColumnarBatchWrite.scala
//  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
//    new WriteBuilder() {
//      override def build(): Write = {
//        new Write {
//          override def toBatch: BatchWrite = {
//            new ColumnarBatchWrite(info, schema, writeConfig)
//          }
//        }
//      }
//    }
//  }
}
