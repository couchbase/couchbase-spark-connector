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
import org.apache.spark.sql.sources.{
  And,
  EqualTo,
  Filter,
  In,
  IsNotNull,
  StringContains,
  StringEndsWith,
  StringStartsWith
}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/** Filters that cannot be compiled to SQL++ must be returned from pushFilters, so that Spark
  * evaluates them, rather than reported as handled and then dropped.
  */
class FilterPushDownTest {
  private val schema = StructType(Seq(StructField("country", StringType)))

  private val supported: Array[Filter] =
    Array(IsNotNull("country"), EqualTo("country", "Austria"))

  // Filter is sealed in Spark 3.5 and every filter it can push down is supported, so there is no
  // unsupported filter to test with here.  (On Spark 4, CollatedFilters are returned to Spark.)
  private val all: Array[Filter] =
    supported :+ And(EqualTo("country", "Austria"), IsNotNull("country"))

  private def assertSplitsFilters(builder: SupportsPushDownFilters): Unit = {
    val returnedToSpark = builder.pushFilters(all)
    assertEquals(Seq(), returnedToSpark.toSeq)
    assertEquals(all.toSeq, builder.pushedFilters().toSeq)
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
    val expected = " `country` IS NOT NULL AND `country` = \"Austria\""
    assertEquals(expected, QueryFilters.compile(supported))
    assertEquals(expected, AnalyticsPartitionReader.compileFilter(supported))
  }

  @Test
  def compileEmptyFilters(): Unit = {
    assertEquals("", QueryFilters.compile(Array.empty))
    assertEquals("", AnalyticsPartitionReader.compileFilter(Array.empty))
  }

  private def assertCompiles(expected: String, filter: Filter): Unit = {
    assertEquals(expected, QueryFilters.compile(Array(filter)))
    assertEquals(expected, AnalyticsPartitionReader.compileFilter(Array(filter)))
  }

  @Test
  def inSupportsNestedFields(): Unit = {
    assertCompiles(
      " `address`.`city` IN [\"Vienna\",\"Graz\"]",
      In("address.city", Array("Vienna", "Graz"))
    )
  }

  @Test
  def stringValuesAreQuoted(): Unit = {
    // Written as a JSON string, so quotes, backslashes and control characters are escaped.
    val value  = "O'Hare \"x\" \\ \n\u0001"
    val quoted = "\"O'Hare \\\"x\\\" \\\\ \\n\\u0001\""
    assertCompiles(s" `name` = $quoted", EqualTo("name", value))
    assertCompiles(s" CONTAINS(`name`, $quoted)", StringContains("name", value))
    assertCompiles(s""" `name` IN [$quoted,"b"]""", In("name", Array(value, "b")))
  }

  @Test
  def likeWildcardsAreEscaped(): Unit = {
    // % _ and \ are escaped with a backslash, which is then doubled inside the string literal.
    // . and * are not special in LIKE, so are left alone.
    val value = "a%b_c\\d.e*"
    assertCompiles(
      " `name` LIKE \"a\\\\%b\\\\_c\\\\\\\\d.e*%\"",
      StringStartsWith("name", value)
    )
    assertCompiles(
      " `name` LIKE \"%a\\\\%b\\\\_c\\\\\\\\d.e*\"",
      StringEndsWith("name", value)
    )
    // CONTAINS is not a pattern match, so wildcards are not escaped.
    assertCompiles(" CONTAINS(`name`, \"a%b_c\\\\d.e*\")", StringContains("name", value))
  }
}
