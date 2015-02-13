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

import org.apache.spark.{SparkConf, SparkContext}

case class CouchbaseConfig(host: String, bucket: String, password: String)

object CouchbaseConfig {

  val DEFAULT_HOST = "127.0.0.1"
  val DEFAULT_BUCKET = "default"
  val DEFAULT_PASSWORD = ""

  def apply(cfg: SparkConf) = {
    // Is it better to throw an exception if people forget to set the configs?
    val host = cfg.get("couchbase.host", DEFAULT_HOST)
    val bucket = cfg.get("couchbase.bucket", DEFAULT_BUCKET)
    val password = cfg.get("couchbase.password", DEFAULT_PASSWORD)
    new CouchbaseConfig(host, bucket, password)
  }

  def apply() = new CouchbaseConfig(DEFAULT_HOST, DEFAULT_BUCKET, DEFAULT_PASSWORD)

}
