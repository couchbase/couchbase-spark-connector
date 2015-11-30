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

case class CouchbaseBucket(name: String, password: String)
case class RetryOptions(maxTries: Int, maxDelay: Int, minDelay: Int)
case class CouchbaseConfig(
  hosts: Seq[String], buckets: Seq[CouchbaseBucket], retryOpts: RetryOptions)

object CouchbaseConfig {

  private val PREFIX = "com.couchbase."
  private val BUCKET_PREFIX = PREFIX + "bucket."
  private val NODES_PREFIX = PREFIX + "nodes"
  private val MAX_RETRIES_PREFIX = PREFIX + "maxRetries"
  private val MAX_RETRY_DELAY_PREFIX = PREFIX + "maxRetryDelay"
  private val MIN_RETRY_DELAY_PREFIX = PREFIX + "minRetryDelay"

  private val COMPAT_PREFIX = "spark.couchbase."
  private val COMPAT_BUCKET_PREFIX = COMPAT_PREFIX + "bucket."
  private val COMPAT_NODES_PREFIX = COMPAT_PREFIX + "nodes"
  private val COMPAT_MAX_RETRIES_PREFIX = COMPAT_PREFIX + "maxRetries"
  private val COMPAT_MAX_RETRY_DELAY_PREFIX = COMPAT_PREFIX + "maxRetryDelay"
  private val COMPAT_MIN_RETRY_DELAY_PREFIX = COMPAT_PREFIX + "minRetryDelay"

  val DEFAULT_NODE = "127.0.0.1"
  val DEFAULT_BUCKET = "default"
  val DEFAULT_PASSWORD = ""
  val DEFAULT_MAX_RETRIES = "130"
  val DEFAULT_MAX_RETRY_DELAY = "1000"
  val DEFAULT_MIN_RETRY_DELAY = "0"

  def apply(cfg: SparkConf) = {
    val bucketConfigs = cfg
      .getAll.to[List]
      .filter(pair => pair._1.startsWith(BUCKET_PREFIX) || pair._1.startsWith(COMPAT_BUCKET_PREFIX))
      .map(pair => CouchbaseBucket(
        pair._1.replace(BUCKET_PREFIX, "").replace(COMPAT_BUCKET_PREFIX, ""),
        pair._2
    ))

    var nodes = cfg.get(NODES_PREFIX, "").split(";")
      .union(cfg.get(COMPAT_NODES_PREFIX, "").split(";"))
      .distinct
      .filter(!_.isEmpty)

    if (nodes.isEmpty) {
      nodes = nodes ++ Array(DEFAULT_NODE)
    }

    val maxRetries = cfg
      .getOption(MAX_RETRIES_PREFIX)
      .orElse(cfg.getOption(COMPAT_MAX_RETRIES_PREFIX))
      .getOrElse(DEFAULT_MAX_RETRIES)
      .toInt

    val minRetryDelay = cfg
      .getOption(MIN_RETRY_DELAY_PREFIX)
      .orElse(cfg.getOption(COMPAT_MIN_RETRY_DELAY_PREFIX))
      .getOrElse(DEFAULT_MIN_RETRY_DELAY)
      .toInt

    val maxRetryDelay = cfg
      .getOption(MAX_RETRY_DELAY_PREFIX)
      .orElse(cfg.getOption(COMPAT_MAX_RETRY_DELAY_PREFIX))
      .getOrElse(DEFAULT_MAX_RETRY_DELAY)
      .toInt

    val retryOptions = RetryOptions(maxRetries, maxRetryDelay, minRetryDelay)

    if (bucketConfigs.isEmpty) {
      new CouchbaseConfig(nodes, Seq(CouchbaseBucket(DEFAULT_BUCKET, DEFAULT_PASSWORD)),
        retryOptions)
    } else {
      new CouchbaseConfig(nodes, bucketConfigs, retryOptions)
    }
  }

  def apply() = new CouchbaseConfig(
    Seq(DEFAULT_NODE),
    Seq(CouchbaseBucket(DEFAULT_BUCKET, DEFAULT_PASSWORD)),
    RetryOptions(DEFAULT_MAX_RETRIES.toInt, DEFAULT_MAX_RETRY_DELAY.toInt,
      DEFAULT_MIN_RETRY_DELAY.toInt)
  )

}
