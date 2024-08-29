/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.spark.connections

import com.couchbase.spark.connections.MultiClusterConnectionTestUtil.runStandardRDDQuery
import com.couchbase.spark.util.SparkOperationalTest
import org.junit.jupiter.api.Test

/** Tests multiple cluster connections where they are dynamically setup, against an empty config and
  * RDD operations.
  */
class MultiClusterConnectionDynamicRDDEmptyConfigIntegrationTest extends SparkOperationalTest {
  override def testName: String = super.testName

  @Test
  def withAllRequiredSettings(): Unit = {
    val id =
      s"couchbase://${infra.params.username}:${infra.params.password}@${infra.params.connectionString}?spark.couchbase.implicitBucket=${infra.params.bucketName}"
    runStandardRDDQuery(spark, id)
  }

  @Test
  def missingImplicitBucket(): Unit = {
    val id = s"couchbase://${}:${infra.params.password}@${infra.params.connectionString}"
    try {
      runStandardRDDQuery(spark, id)
    } catch {
      case _: IllegalArgumentException =>
    }
  }
}
