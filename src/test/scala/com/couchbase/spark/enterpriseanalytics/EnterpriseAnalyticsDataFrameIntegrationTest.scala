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

import com.couchbase.spark.enterpriseanalytics.util.{
  EnterpriseAnalyticsTestUtil,
  SparkSimpleEnterpriseAnalyticsTest
}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class EnterpriseAnalyticsDataFrameIntegrationTest extends SparkSimpleEnterpriseAnalyticsTest {
  @Test
  def testPushDownAggregationWithoutGroupBy(): Unit = {
    val airlines = EnterpriseAnalyticsTestUtil.basicDataFrameReader(spark).load()

    airlines.createOrReplaceTempView("airlines")

    val aggregates = spark.sql("select max(id) as max_id, min(name) as min_name from airlines")

    aggregates.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation =>
      assertTrue(p.toString().contains("MAX(`id`)"))
      assertTrue(p.toString().contains("MIN(`name`)"))
    }

    assertTrue(aggregates.count() >= 1)
    assertNotNull(aggregates.first().getAs[String]("max_id"))
    assertNotNull(aggregates.first().getAs[String]("min_name"))
  }

  @Test
  def testPushDownAggregationWithGroupBy(): Unit = {
    val airlines = EnterpriseAnalyticsTestUtil.basicDataFrameReader(spark).load()

    airlines.createOrReplaceTempView("airlines")

    val aggregates = spark.sql(
      "select max(id) as max_id, min(name) as min_name, country from airlines group by country"
    )

    aggregates.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation =>
      assertTrue(p.toString().contains("country"))
      assertTrue(p.toString().contains("MAX(`id`)"))
      assertTrue(p.toString().contains("MIN(`name`)"))
    }

    assertTrue(aggregates.count() >= 1)
    assertNotNull(aggregates.first().getAs[String]("max_id"))
    assertNotNull(aggregates.first().getAs[String]("min_name"))
    assertNotNull(aggregates.first().getAs[String]("country"))
  }
}
