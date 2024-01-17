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
package com.couchbase.spark.util

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, TestInstance}
import org.slf4j.LoggerFactory

import java.util.UUID

@TestInstance(Lifecycle.PER_CLASS)
class SparkTest {
  protected var spark: SparkSession = _
  protected var infra: TestInfraConnectedToSpark = _
  protected val logger = LoggerFactory.getLogger(classOf[SparkTest])

  def testName = {
    val st = Thread.currentThread.getStackTrace
    st.map(_.getClassName.replace("com.couchbase.spark.", ""))
      .filterNot(_.endsWith("SparkTest"))
      .find(_.endsWith("Test"))
      .getOrElse(st.map(_.getMethodName)
        .find(v => v.endsWith("Test") || v.endsWith("Test$1"))
        .getOrElse(UUID.randomUUID.toString.substring(0, 6)))
  }

  // Tests can override if needed.
  def sparkBuilderCustomizer(builder: SparkSession.Builder, params: Params): Unit = {
  }

  @BeforeAll
  def setup(): Unit = {
    infra = TestInfraBuilder()
      .createBucketScopeAndCollection(testName)
      .connectToSpark()
      .prepareAirportSampleData()
    spark = infra.spark
  }

  @AfterAll
  def teardown(): Unit = {
    infra.stop()
  }
}
