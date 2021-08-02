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

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class QueryScanBuilder(schema: StructType, readConfig: QueryReadConfig)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var projections = schema
  private var pushedFilter = Array.empty[Filter]

  override def build(): Scan = {
    new QueryScan(projections, readConfig, pushedFilter)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushedFilter = filters
    Array.empty[Filter]
  }

  override def pushedFilters(): Array[Filter] = pushedFilter
  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (requiredSchema != null && requiredSchema.nonEmpty) {
      projections = requiredSchema
    }
  }
}
