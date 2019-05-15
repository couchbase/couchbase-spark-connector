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

import org.apache.spark.SparkConf
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class CouchbaseConfigSpec extends FlatSpec with Matchers {

  "A compat config containing nodes" should " merge with a regular nodes" in {
    val sparkConf = new SparkConf()
      .set("com.couchbase.nodes", "a;b;c;d")
      .set("spark.couchbase.nodes", "d;e;f")

    val cbConf = CouchbaseConfig.apply(sparkConf)

    cbConf.hosts should equal(Array("a", "b", "c", "d", "e", "f"))
  }

  it should "accept compat bucket references in addition to regular ones" in {
    val sparkConf = new SparkConf()
      .set("com.couchbase.bucket.a", "b")
      .set("spark.couchbase.bucket.c", "d")

    val cbConf = CouchbaseConfig.apply(sparkConf)

    cbConf.buckets.size should equal(2)

    cbConf.buckets.head.name should equal("a")
    cbConf.buckets.head.password should equal("b")
    cbConf.buckets(1).name should equal("c")
    cbConf.buckets(1).password should equal("d")
  }

  it should "apply default settings" in {
    val sparkConf = new SparkConf()

    val cbConf = CouchbaseConfig.apply(sparkConf)
    cbConf.hosts should equal (Array("127.0.0.1"))
    cbConf.buckets.size should equal(1)
    cbConf.buckets.head.name should equal("default")
    cbConf.buckets.head.password should equal("")
  }

  it should "work only with com prefix settings" in {
    val sparkConf = new SparkConf()
      .set("com.couchbase.nodes", "a;b;c;d")

    val cbConf = CouchbaseConfig.apply(sparkConf)
    cbConf.hosts should equal(Array("a", "b", "c", "d"))
  }

  it should "work only with spark prefix settings" in {
    val sparkConf = new SparkConf()
      .set("spark.couchbase.nodes", "d;e;f")

    val cbConf = CouchbaseConfig.apply(sparkConf)
    cbConf.hosts should equal(Array("d", "e", "f"))
  }

  it should "apply default settings with empty list on com prefix" in {
    val sparkConf = new SparkConf()
      .set("com.couchbase.nodes", "")

    val cbConf = CouchbaseConfig.apply(sparkConf)
    cbConf.hosts should equal(Array("127.0.0.1"))
  }

  it should "apply default settings with empty list on spark prefix" in {
    val sparkConf = new SparkConf()
      .set("spark.couchbase.nodes", "")

    val cbConf = CouchbaseConfig.apply(sparkConf)
    cbConf.hosts should equal(Array("127.0.0.1"))
  }

  it should "apply default retry values" in {
    val sparkConf = new SparkConf()
    val cbConf = CouchbaseConfig.apply(sparkConf)

    cbConf.retryOpts should equal(RetryOptions(130, 1000, 0))
  }

  it should "set custom retry values on com prefix" in {
    val sparkConf = new SparkConf()
      .set("com.couchbase.maxRetries", "5")
      .set("com.couchbase.maxRetryDelay", "10")
      .set("com.couchbase.minRetryDelay", "1")
    val cbConf = CouchbaseConfig.apply(sparkConf)

    cbConf.retryOpts should equal(RetryOptions(5, 10, 1))
  }

  it should "set custom retry values on spark prefix" in {
    val sparkConf = new SparkConf()
      .set("spark.couchbase.maxRetries", "5")
      .set("spark.couchbase.maxRetryDelay", "10")
      .set("spark.couchbase.minRetryDelay", "1")
    val cbConf = CouchbaseConfig.apply(sparkConf)

    cbConf.retryOpts should equal(RetryOptions(5, 10, 1))
  }
}
