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

/**
  * Many tests just need to create a SparkSession, connected to a Couchbase cluster, and have a collection created
  * with some basic test data.
  *
  * This class is for that simple use-case, and that only.  Many tests should not extend this class - instead they
  * should copy the lines below that they need.
  */
@TestInstance(Lifecycle.PER_CLASS)
abstract class SparkSimpleTest {
  protected var spark: SparkSession = _
  protected val params = new TestOptionsPropertyLoader()
  protected val testResourceCreator = new TestResourceCreator(params)
  protected var testResources: CreatedResources = _

  // Makes it easier to track created resources
  def testName: String

  @BeforeAll
  def setup(): Unit = {
    val builder = SparkSessionHelper.sparkSessionBuilder()
    testResources = testResourceCreator.createBucketScopeAndCollection(Some(testName))
    spark = SparkSessionHelper.provideCouchbaseCreds(builder, params)
      // Too many original tests were built around the assumption of this implicitBucket being present to change it.
      // Per the class comments - if this class does not suit as-is, please copy the required building blocks into your
      // test file and modify there.
      .config("spark.couchbase.implicitBucket", testResources.bucketName)
      .getOrCreate()
    testResourceCreator.prepareAirportSampleData(spark, testResources)
  }

  @AfterAll
  def teardown(): Unit = {
    spark.stop()
    testResourceCreator.deleteBucket(testResources.bucketName)
    testResourceCreator.stop()
  }
}
