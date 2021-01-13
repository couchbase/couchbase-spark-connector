/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.spark.rdd

import com.couchbase.client.scala.query.QueryScanConsistency.RequestPlus
import com.couchbase.spark.test.{Capabilities, ClusterType, IgnoreWhen, SparkIntegrationTest}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{Test, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle
import com.couchbase.spark._

@IgnoreWhen(clusterTypes = Array(ClusterType.MOCKED), missesCapabilities = Array(Capabilities.QUERY))
@TestInstance(Lifecycle.PER_CLASS)
class QueryRDDSpec extends SparkIntegrationTest {

  @Test
  def runsQuery(): Unit = {
    val rdd = sparkSession().sparkContext.couchbaseQuery("select 1=1", CouchbaseQueryOptions(scanConsistency = Some(RequestPlus())))

    val results = rdd.collect()
    assertTrue(results(0).rows.head.bool("$1"))
  }

}
