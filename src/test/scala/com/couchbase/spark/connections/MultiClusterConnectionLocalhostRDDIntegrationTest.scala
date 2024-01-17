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
import com.couchbase.spark.util.SparkTest
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._

/** Tests multiple cluster connections where they are dynamically setup, against a regular config,
  * RDD operations and a localhost cluster.
  *
  * These tests are for manual testing and hence are disabled.
  */
@Disabled
class MultiClusterConnectionLocalhostRDDIntegrationTest extends SparkTest {
  override def testName: String = super.testName

  @Test
  def missingPort(): Unit = {
    val id =
      s"couchbase://Administrator:password@localhost?spark.couchbase.implicitBucket=${infra.params.bucketName}"
    runStandardRDDQuery(spark, id)
  }
}
