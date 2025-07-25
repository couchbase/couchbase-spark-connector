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
package com.couchbase.spark.enterpriseanalytics.util

import com.couchbase.spark.enterpriseanalytics.EnterpriseAnalyticsOptions
import org.apache.spark.sql.{DataFrameReader, SparkSession}

object EnterpriseAnalyticsTestUtil {
  def basicDataFrameReader(spark: SparkSession): DataFrameReader = {
    spark.read
      .format("couchbase.enterprise-analytics")
      .option(EnterpriseAnalyticsOptions.Database, "travel-sample")
      .option(EnterpriseAnalyticsOptions.Scope, "inventory")
      .option(EnterpriseAnalyticsOptions.Collection, "airline")
  }
}
