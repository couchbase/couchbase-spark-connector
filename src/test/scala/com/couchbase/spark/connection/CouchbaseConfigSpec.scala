/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark.connection

import org.apache.spark.SparkConf
import org.scalatest.{Matchers, FlatSpec}

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
