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

import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer, CouchbaseService}

object ClusterVersions {
  // Pegging it to the current latest.
  val DefaultTestVersion = "couchbase/server:7.6.2"

  def testContainer(): CouchbaseContainer = {
    new CouchbaseContainer(DefaultTestVersion)
      .withServiceQuota(CouchbaseService.KV, 1024)
      .withStartupAttempts(10)
  }

  def testContainer(bucketName: String): CouchbaseContainer = {
    testContainer()
      .withBucket(new BucketDefinition(bucketName))
  }
}