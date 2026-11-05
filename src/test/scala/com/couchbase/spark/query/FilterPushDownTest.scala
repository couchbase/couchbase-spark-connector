/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import com.couchbase.spark.analytics.{
  AnalyticsPartitionReader,
  AnalyticsReadConfig,
  AnalyticsScanBuilder
}
import com.couchbase.spark.columnar.{ColumnarReadConfig, ColumnarScanBuilder}
import com.couchbase.spark.enterpriseanalytics.{
  EnterpriseAnalyticsReadConfig,
  EnterpriseAnalyticsScanBuilder
}
import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.sources.{And, CollatedEqualTo, EqualTo, Filter, In, IsNotNull}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

/** Filters that cannot be compiled to SQL++ must be returned from pushFilters, so that Spark
  * evaluates them, rather than reported as handled and then dropped.
  */
class FilterPushDownTest {
  private val schema = StructType(Seq(StructField("country", StringType)))

  private val supported: Array[Filter] =
    Array(IsNotNull("country"), EqualTo("country", "Austria"))

  // Spark 4 produces CollatedFilters for comparisons on collated string columns.
  private val collated: Filter = CollatedEqualTo("country", "austria", StringType("UTF8_LCASE"))

  // A filter is pushed down whole or not at all, so one containing an unsupported child is unsupported.
  private val containsCollated: Filter = And(EqualTo("country", "Austria"), collated)

  private val all: Array[Filter] = supported ++ Array(collated, containsCollated)

  private def assertSplitsFilters(builder: SupportsPushDownFilters): Unit = {
    val returnedToSpark = builder.pushFilters(all)
    assertEquals(Seq(collated, containsCollated), returnedToSpark.toSeq)
    assertEquals(supported.toSeq, builder.pushedFilters().toSeq)
  }

  @Test
  def queryScanBuilder(): Unit = {
    val readConfig = QueryReadConfig(
      "bucket",
      None,
      None,
      "__META_ID",
      "__META_CAS",
      outputCas = false,
      None,
      QueryOptions.NotBoundedScanConsistency,
      None,
      pushDownAggregate = true,
      None,
      None
    )
    assertSplitsFilters(new QueryScanBuilder(schema, readConfig))
  }

  @Test
  def analyticsScanBuilder(): Unit = {
    val readConfig = AnalyticsReadConfig(
      "dataset",
      None,
      None,
      "__META_ID",
      None,
      "notBounded",
      None,
      pushDownAggregate = true,
      None
    )
    assertSplitsFilters(new AnalyticsScanBuilder(schema, readConfig))
  }

  @Test
  def columnarScanBuilder(): Unit = {
    val readConfig =
      ColumnarReadConfig("collection", "database", "scope", None, "notBounded", None, None)
    assertSplitsFilters(new ColumnarScanBuilder(schema, readConfig))
  }

  @Test
  def enterpriseAnalyticsScanBuilder(): Unit = {
    val readConfig =
      EnterpriseAnalyticsReadConfig("database", "scope", "collection", None, "notBounded", None)
    assertSplitsFilters(new EnterpriseAnalyticsScanBuilder(schema, readConfig))
  }

  @Test
  def compileSupportedFilters(): Unit = {
    val expected = " `country` IS NOT NULL AND `country` = 'Austria'"
    assertEquals(expected, QueryFilters.compile(supported))
    assertEquals(expected, AnalyticsPartitionReader.compileFilter(supported))
  }

  @Test
  def compileEmptyFilters(): Unit = {
    assertEquals("", QueryFilters.compile(Array.empty))
    assertEquals("", AnalyticsPartitionReader.compileFilter(Array.empty))
  }

  // Unsupported filters should never reach compile.  If one does, fail rather than silently drop it.
  @Test
  def compileRejectsUnsupportedFilters(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => QueryFilters.compile(Array(collated))
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => AnalyticsPartitionReader.compileFilter(Array(collated))
    )
  }

  private def assertCompiles(expected: String, filter: Filter): Unit = {
    assertEquals(expected, QueryFilters.compile(Array(filter)))
    assertEquals(expected, AnalyticsPartitionReader.compileFilter(Array(filter)))
  }

  @Test
  def inSupportsNestedFields(): Unit = {
    assertCompiles(
      " `address`.`city` IN ['Vienna','Graz']",
      In("address.city", Array("Vienna", "Graz"))
    )
  }
}
