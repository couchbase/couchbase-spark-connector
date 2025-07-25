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

import com.couchbase.spark.enterpriseanalytics.util.EnterpriseAnalyticsTestUtil.basicDataFrameReader
import com.couchbase.spark.enterpriseanalytics.util.SparkSimpleEnterpriseAnalyticsTest
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test

case class Airline(id: String, name: String, country: String)

class EnterpriseAnalyticsBasicsIntegrationTest extends SparkSimpleEnterpriseAnalyticsTest {
  @Test
  def basicRead(): Unit = {
    val airlines = basicDataFrameReader(spark)
      .load()

    assertTrue(airlines.count >= 1)
    airlines.foreach(row => {
      assertNotNull(row.getAs[String]("id"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

  @Test
  def readDataSet(): Unit = {
    val sparkSession = spark
    import sparkSession.implicits._

    val airlines = basicDataFrameReader(spark).load().as[Airline]

    assertTrue(airlines.count >= 1)
  }

  @Test
  def readSql(): Unit = {
    val airlines = basicDataFrameReader(spark)
      .load()
    airlines.createOrReplaceTempView("airlinesView")
    val airlinesFromView = spark.sql("SELECT * FROM airlinesView")

    assertEquals(airlines.count, airlinesFromView.count)
  }

  @Test
  def withFilter(): Unit = {
    val airlinesUnfiltered = basicDataFrameReader(spark).load
    val airlinesFiltered = basicDataFrameReader(spark)
      .option(EnterpriseAnalyticsOptions.Filter, "country = 'United States'")
      .load

    assertTrue(airlinesFiltered.count() < airlinesUnfiltered.count())
  }

  @Test
  def setAllOptions(): Unit = {
    val airlines = basicDataFrameReader(spark)
      .option(EnterpriseAnalyticsOptions.Filter, "country = 'United States'")
      .option(EnterpriseAnalyticsOptions.InferLimit, "500")
      .option(
        EnterpriseAnalyticsOptions.ScanConsistency,
        EnterpriseAnalyticsOptions.NotBoundedScanConsistency
      )
      .option(EnterpriseAnalyticsOptions.Timeout, "10s")
      .load

    assertTrue(airlines.count >= 1)
  }
}
