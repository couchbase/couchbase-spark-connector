/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.spark.connection

import org.scalatest._

class CouchbaseConnectionSpec extends FlatSpec with Matchers {

  "A Connection" should "not be initialized more than once" in {
    val conn1 = CouchbaseConnection()
    val conn2 = CouchbaseConnection()

    conn1 should equal (conn2)
  }

  it should "maintain bucket references" in {
    val conn = CouchbaseConnection()
    val cfg = CouchbaseConfig()

    val bucket1 = conn.bucket(cfg, "default")
    val bucket2 = conn.bucket(cfg, "default")

    bucket1 should equal (bucket2)
  }

}
