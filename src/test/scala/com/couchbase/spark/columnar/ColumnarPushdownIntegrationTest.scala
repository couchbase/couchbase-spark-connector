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

import com.couchbase.spark.columnar.util.ColumnarTestUtil.basicDataFrameReader
import com.couchbase.spark.util.SparkColumnarTest
import org.apache.spark.sql.Dataset
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull}
import org.junit.jupiter.api.Test

class ColumnarPushdownIntegrationTest extends SparkColumnarTest {
  override def testName: String = super.testName

  @Test
  def pushdownLimit(): Unit = {
    val airlines = basicDataFrameReader(spark)
      .load()
      .limit(5)

    /*
    Without pushdownLimit support:
      == Physical Plan ==
      CollectLimit 5
      +- *(1) Project [callsign#19, country#20, iata#21, icao#22, id#23, name#24, type#25]
         +- BatchScan travel-sample.inventory.airline[callsign#19, country#20, iata#21, icao#22, id#23, name#24, type#25] class com.couchbase.spark.columnar.ColumnarScan RuntimeFilters: []

    With pushdownLimit support:
      == Physical Plan ==
      *(1) Project [callsign#19, country#20, iata#21, icao#22, id#23, name#24, type#25]
      +- BatchScan travel-sample.inventory.airline[callsign#19, country#20, iata#21, icao#22, id#23, name#24, type#25] class com.couchbase.spark.columnar.ColumnarScan RuntimeFilters: []
     */

    assertEquals(5, airlines.count)
    assertFalse(airlines.asInstanceOf[Dataset[Airline]].queryExecution.executedPlan.toString.toLowerCase.contains("limit"))
  }

  // No way to automatically validate that pushdown occurred, but have manually verified.
  @Test
  def pushdownCount(): Unit = {
    val airlinesCount = basicDataFrameReader(spark)
      .load()
      .count()

    assertEquals(187, airlinesCount)
  }
}
