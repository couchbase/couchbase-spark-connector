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

case class CouchbaseBucket(name: String, password: String)
case class Credential(username: String, password: String)
case class RetryOptions(maxTries: Int, maxDelay: Int, minDelay: Int)
case class SslOptions(enabled: Boolean, keystorePath: String, keystorePassword: String)
case class Timeouts(query: Option[Long], view: Option[Long], search: Option[Long],
                    analytics: Option[Long], kv: Option[Long], connect: Option[Long],
                    disconnect: Option[Long], management: Option[Long])
case class CouchbaseConfig(hosts: Seq[String], buckets: Seq[CouchbaseBucket],
                           retryOpts: RetryOptions, sslOptions: Option[SslOptions],
                           credential: Option[Credential], timeouts: Timeouts,
                           dnsSrvEnabled: Boolean)

object CouchbaseConfig {

  private val SPARK_PREFIX = "spark."
  private val SPARK_SSL_PREFIX = SPARK_PREFIX + "ssl."
  private val SPARK_SSL_ENABLED = SPARK_SSL_PREFIX + "enabled"
  private val SPARK_SSL_KEYSTORE = SPARK_SSL_PREFIX + "keyStore"
  private val SPARK_SSL_KEYSTORE_PASSWORD = SPARK_SSL_PREFIX + "keyStorePassword"

  private val PREFIX = "com.couchbase."
  private val BUCKET_PREFIX = PREFIX + "bucket."
  private val NODES_PREFIX = PREFIX + "nodes"
  private val MAX_RETRIES_PREFIX = PREFIX + "maxRetries"
  private val MAX_RETRY_DELAY_PREFIX = PREFIX + "maxRetryDelay"
  private val MIN_RETRY_DELAY_PREFIX = PREFIX + "minRetryDelay"
  private val SSL_ENABLED = PREFIX + "sslEnabled"
  private val SSL_KEYSTORE_FILE = PREFIX + "sslKeyStore"
  private val SSL_KEYSTORE_PASSWORD = PREFIX + "sslKeyStorePassword"
  private val USERNAME = PREFIX + "username"
  private val PASSWORD = PREFIX + "password"
  private val QUERY_TIMEOUT = PREFIX + "queryTimeout"
  private val VIEW_TIMEOUT = PREFIX + "viewTimeout"
  private val SEARCH_TIMEOUT = PREFIX + "searchTimeout"
  private val ANALYTICS_TIMEOUT = PREFIX + "analyticsTimeout"
  private val KV_TIMEOUT = PREFIX + "kvTimeout"
  private val CONNECT_TIMEOUT = PREFIX + "connectTimeout"
  private val MANAGEMENT_TIMEOUT = PREFIX + "managementTimeout"
  private val DISCONNECT_TIMEOUT = PREFIX + "disconnectTimeout"
  private val DNS_SRV_ENABLED = PREFIX + "dnsSrvEnabled"

  private val COMPAT_PREFIX = SPARK_PREFIX + "couchbase."
  private val COMPAT_BUCKET_PREFIX = COMPAT_PREFIX + "bucket."
  private val COMPAT_NODES_PREFIX = COMPAT_PREFIX + "nodes"
  private val COMPAT_MAX_RETRIES_PREFIX = COMPAT_PREFIX + "maxRetries"
  private val COMPAT_MAX_RETRY_DELAY_PREFIX = COMPAT_PREFIX + "maxRetryDelay"
  private val COMPAT_MIN_RETRY_DELAY_PREFIX = COMPAT_PREFIX + "minRetryDelay"
  private val COMPAT_SSL_ENABLED = COMPAT_PREFIX + "sslEnabled"
  private val COMPAT_SSL_KEYSTORE_FILE = COMPAT_PREFIX + "sslKeyStore"
  private val COMPAT_SSL_KEYSTORE_PASSWORD = COMPAT_PREFIX + "sslKeyStorePassword"
  private val COMPAT_USERNAME = COMPAT_PREFIX + "username"
  private val COMPAT_PASSWORD = COMPAT_PREFIX + "password"
  private val COMPAT_QUERY_TIMEOUT = COMPAT_PREFIX + "queryTimeout"
  private val COMPAT_VIEW_TIMEOUT = COMPAT_PREFIX + "viewTimeout"
  private val COMPAT_SEARCH_TIMEOUT = COMPAT_PREFIX + "searchTimeout"
  private val COMPAT_ANALYTICS_TIMEOUT = COMPAT_PREFIX + "analyticsTimeout"
  private val COMPAT_KV_TIMEOUT = COMPAT_PREFIX + "kvTimeout"
  private val COMPAT_CONNECT_TIMEOUT = COMPAT_PREFIX + "connectTimeout"
  private val COMPAT_MANAGEMENT_TIMEOUT = COMPAT_PREFIX + "managementTimeout"
  private val COMPAT_DISCONNECT_TIMEOUT = COMPAT_PREFIX + "disconnectTimeout"
  private val COMPAT_DNS_SRV_ENABLED = COMPAT_PREFIX + "dnsSrvEnabled"

  val DEFAULT_NODE = "127.0.0.1"
  val DEFAULT_BUCKET = "default"
  val DEFAULT_PASSWORD = ""
  val DEFAULT_MAX_RETRIES = "130"
  val DEFAULT_MAX_RETRY_DELAY = "1000"
  val DEFAULT_MIN_RETRY_DELAY = "0"

  def apply(cfg: SparkConf) = {
    val username = cfg
      .getOption(USERNAME)
      .orElse(cfg.getOption(COMPAT_USERNAME))

    val password = cfg
      .getOption(PASSWORD)
      .orElse(cfg.getOption(COMPAT_PASSWORD))

    val credential = if (username.isDefined) {
      Some(Credential(username.get, password.get))
    } else {
      None
    }

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

    val dnsSrvEnabled = cfg
      .getOption(DNS_SRV_ENABLED)
      .orElse(cfg.getOption(COMPAT_DNS_SRV_ENABLED))
      .getOrElse("false")
      .toBoolean

    val retryOptions = RetryOptions(maxRetries, maxRetryDelay, minRetryDelay)


    var useSsl = false
    var keyStorePath = ""
    var keyStorePassword = ""

    // check for spark-related SSL settings
    if (cfg.get(SPARK_SSL_ENABLED, "false").toBoolean) {
      useSsl = true
      keyStorePath = cfg.get(SPARK_SSL_KEYSTORE)
      keyStorePassword = cfg.get(SPARK_SSL_KEYSTORE_PASSWORD)
    }

    // now override (if set) with the couchbase-specific values
    if (cfg.contains(SSL_ENABLED)) {
      useSsl = cfg.get(SSL_ENABLED).toBoolean
      if (useSsl) {
        keyStorePath = cfg.get(SSL_KEYSTORE_FILE)
        keyStorePassword = cfg.get(SSL_KEYSTORE_PASSWORD)
      }
    }
    if (cfg.contains(COMPAT_SSL_ENABLED)) {
      useSsl = cfg.get(COMPAT_SSL_ENABLED).toBoolean
      if (useSsl) {
        keyStorePath = cfg.get(COMPAT_SSL_KEYSTORE_FILE)
        keyStorePassword = cfg.get(COMPAT_SSL_KEYSTORE_PASSWORD)
      }
    }

    val sslOptions = if (useSsl) {
      Some(SslOptions(enabled = true, keyStorePath, keyStorePassword))
    } else {
      None
    }

    val timeouts = parseTimeouts(cfg)

    if (bucketConfigs.isEmpty) {
      new CouchbaseConfig(nodes, Seq(CouchbaseBucket(DEFAULT_BUCKET, DEFAULT_PASSWORD)),
        retryOptions, sslOptions, credential, timeouts, dnsSrvEnabled)
    } else {
      new CouchbaseConfig(nodes, bucketConfigs, retryOptions, sslOptions, credential, timeouts,
        dnsSrvEnabled)
    }
  }

  def parseTimeouts(cfg: SparkConf): Timeouts = {
    Timeouts(
      parseTimeout(cfg, QUERY_TIMEOUT, COMPAT_QUERY_TIMEOUT),
      parseTimeout(cfg, VIEW_TIMEOUT, COMPAT_VIEW_TIMEOUT),
      parseTimeout(cfg, SEARCH_TIMEOUT, COMPAT_SEARCH_TIMEOUT),
      parseTimeout(cfg, ANALYTICS_TIMEOUT, COMPAT_ANALYTICS_TIMEOUT),
      parseTimeout(cfg, KV_TIMEOUT, COMPAT_KV_TIMEOUT),
      parseTimeout(cfg, CONNECT_TIMEOUT, COMPAT_CONNECT_TIMEOUT),
      parseTimeout(cfg, DISCONNECT_TIMEOUT, COMPAT_DISCONNECT_TIMEOUT),
      parseTimeout(cfg, MANAGEMENT_TIMEOUT, COMPAT_MANAGEMENT_TIMEOUT)
    )
  }

  def parseTimeout(cfg: SparkConf, input: String, compat: String): Option[Long] = {
    cfg.getOption(input).orElse(cfg.getOption(compat)).map(_.toLong)
  }

  def apply(creds: Credential) = new CouchbaseConfig(
    Seq(DEFAULT_NODE),
    Seq(CouchbaseBucket(DEFAULT_BUCKET, DEFAULT_PASSWORD)),
    RetryOptions(DEFAULT_MAX_RETRIES.toInt, DEFAULT_MAX_RETRY_DELAY.toInt,
      DEFAULT_MIN_RETRY_DELAY.toInt), None, Some(creds),
    Timeouts(None, None, None, None, None, None, None, None),
    false
  )

}
