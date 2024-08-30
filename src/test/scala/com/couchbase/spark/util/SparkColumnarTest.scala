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
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeAll, TestInstance}

// Assumptions:
// The travel-sample dataset has been loaded
@deprecated("Deprecating: tests should use the simpler replacement classes (TestResourceCreator, SparkSessionHelper, and possibly - but not required - SparkSimpleTest")
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(Array(classOf[RequiresColumnarCluster]))
class SparkColumnarTest extends SparkTest {

  // Tests can override if needed.
  override def sparkBuilderCustomizer(builder: SparkSession.Builder, params: Params): Unit = {

  }

  @BeforeAll
  override def setup(): Unit = {
    infra = TestInfraBuilder()
      .connectToSpark((builder, params) => {
        builder
          .config("spark.couchbase.connectionString", params.connectionString)
          .config("spark.couchbase.username", params.username)
          .config("spark.couchbase.password", params.password)
          // Wouldn't be necessary against a prod Columnar cluster, but helps with testing against dev clusters
          .config("spark.ssl.insecure", "true")

      })
    spark = infra.spark
  }
}
