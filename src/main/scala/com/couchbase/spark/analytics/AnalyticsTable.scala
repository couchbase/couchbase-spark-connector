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

import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.CouchbaseConfig
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import java.util

class AnalyticsTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String],
                     couchbaseConfig: CouchbaseConfig)
  extends SupportsRead {

  override def name(): String = {
   if (couchbaseConfig.dsConfig.bucket.isEmpty || couchbaseConfig.dsConfig.scope.isEmpty) {
     couchbaseConfig.dsConfig.dataset.get
   } else {
     couchbaseConfig.dsConfig.bucket + ":" +
       couchbaseConfig.dsConfig.scope.getOrElse(DefaultConstants.DefaultScopeName) + ":" +
       couchbaseConfig.dsConfig.dataset.get
   }
  }
  override def schema(): StructType = schema
  override def partitioning(): Array[Transform] = partitioning
  override def properties(): util.Map[String, String] = properties
  override def capabilities(): util.Set[TableCapability] =
    Set[TableCapability](TableCapability.BATCH_READ).asJava
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new AnalyticsScanBuilder(schema, couchbaseConfig)

}
