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

import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class EnterpriseAnalyticsTable(
    schema: StructType,
    readConfig: EnterpriseAnalyticsReadConfig
) extends SupportsRead {
  override def name(): String = {
    s"EnterpriseAnalyticsTable"
  }
  override def schema(): StructType = schema
  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new EnterpriseAnalyticsScanBuilder(schema, readConfig)

}
