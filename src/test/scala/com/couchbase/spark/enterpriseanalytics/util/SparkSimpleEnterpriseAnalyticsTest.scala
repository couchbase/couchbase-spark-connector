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

import com.couchbase.spark.util.{SparkSessionHelper, TestOptionsPropertyLoader}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{AfterAll, BeforeAll, TestInstance}

/** Many tests just need to create a SparkSession, connected to an Enterprise Analytics cluster, and
  * have a collection created with some basic test data.
  *
  * This class is for that simple use-case, and that only. Many tests should not extend this class -
  * instead they should copy the lines below that they need.
  *
  * All Enterprise Analytics tests assume the travel-sample database is loaded (since we don't have
  * an easy way with Spark to write data).
  */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(Array(classOf[RequiresEnterpriseAnalyticsCluster]))
abstract class SparkSimpleEnterpriseAnalyticsTest {
  protected var spark: SparkSession = _
  protected val params              = new TestOptionsPropertyLoader()

  @BeforeAll
  def setup(): Unit = {
    val builder = SparkSessionHelper.sparkSessionBuilder()
    spark = SparkSessionHelper.provideCouchbaseCreds(builder, params).getOrCreate()
  }

  @AfterAll
  def teardown(): Unit = {
    spark.stop()
  }
}
