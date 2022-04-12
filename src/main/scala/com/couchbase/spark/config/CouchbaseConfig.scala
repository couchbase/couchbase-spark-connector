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

package com.couchbase.spark.config

import org.apache.spark.SparkConf

import java.util.Locale

case class Credentials(username: String, password: String)

case class SparkSslOptions(enabled: Boolean, keystorePath: String, keystorePassword: String)

case class CouchbaseConfig(
  connectionString: String,
  credentials: Credentials,
  bucketName: Option[String],
  scopeName: Option[String],
  collectionName: Option[String],
  waitUntilReadyTimeout: Option[String],
  sparkSslOptions: SparkSslOptions,
  properties: Seq[(String, String)]
) {

  def implicitBucketNameOr(explicitName: String): String = {
    var bn = Option(explicitName)
    if (bn.isEmpty && bucketName.isEmpty) {
      throw new IllegalArgumentException("Either a implicitBucket needs to be configured, " +
        "or a bucket name needs to be provided as part of the options!")
    } else if (bn.isEmpty) {
      bn = bucketName
    }
    bn.get
  }

  def implicitScopeNameOr(explicitName: String): Option[String] = {
    var sn = Option(explicitName)
    if (sn.isEmpty && scopeName.isDefined) {
      sn = scopeName
    }
    sn
  }

  def implicitCollectionName(explicitName: String): Option[String] = {
    var cn = Option(explicitName)
    if (cn.isEmpty && collectionName.isDefined) {
      cn = collectionName
    }
    cn
  }

  def dcpConnectionString(): String = {
    val lowerCasedConnstr = connectionString.toLowerCase(Locale.ROOT)

    if (lowerCasedConnstr.startsWith("couchbase://") || lowerCasedConnstr.startsWith("couchbases://")) {
      connectionString
    } else {
      if (sparkSslOptions.enabled) {
        "couchbases://" + connectionString
      } else {
        "couchbase://" + connectionString
      }
    }
  }

}

object CouchbaseConfig {

  private val SPARK_PREFIX = "spark."

  private val PREFIX = SPARK_PREFIX + "couchbase."
  private val USERNAME = PREFIX + "username"
  private val PASSWORD = PREFIX + "password"
  private val CONNECTION_STRING = PREFIX + "connectionString"
  private val BUCKET_NAME = PREFIX + "implicitBucket"
  private val SCOPE_NAME = PREFIX + "implicitScope"
  private val COLLECTION_NAME = PREFIX + "implicitCollection"
  private val WAIT_UNTIL_READY_TIMEOUT = PREFIX + "waitUntilReadyTimeout"

  private val SPARK_SSL_PREFIX = SPARK_PREFIX + "ssl."
  private val SPARK_SSL_ENABLED = SPARK_SSL_PREFIX + "enabled"
  private val SPARK_SSL_KEYSTORE = SPARK_SSL_PREFIX + "keyStore"
  private val SPARK_SSL_KEYSTORE_PASSWORD = SPARK_SSL_PREFIX + "keyStorePassword"

  def checkRequiredProperties(cfg: SparkConf): Unit = {
    if (!cfg.contains(CONNECTION_STRING)) {
      throw new IllegalArgumentException("Required config property " + CONNECTION_STRING + " is not present")
    }
    if (!cfg.contains(USERNAME)) {
      throw new IllegalArgumentException("Required config property " + USERNAME + " is not present")
    }
    if (!cfg.contains(PASSWORD)) {
      throw new IllegalArgumentException("Required config property " + PASSWORD + " is not present")
    }
  }

  def apply(cfg: SparkConf): CouchbaseConfig = {
    checkRequiredProperties(cfg)

    val connectionString = cfg.get(CONNECTION_STRING)

    val username = cfg.get(USERNAME)
    val password = cfg.get(PASSWORD)
    val credentials = Credentials(username, password)

    val bucketName = cfg.getOption(BUCKET_NAME)
    val scopeName = cfg.getOption(SCOPE_NAME)
    val collectionName = cfg.getOption(COLLECTION_NAME)
    val waitUntilReadyTimeout = cfg.getOption(WAIT_UNTIL_READY_TIMEOUT)

    var useSsl = false
    var keyStorePath = ""
    var keyStorePassword = ""

    // check for spark-related SSL settings
    if (cfg.get(SPARK_SSL_ENABLED, "false").toBoolean) {
      useSsl = true
      keyStorePath = cfg.get(SPARK_SSL_KEYSTORE)
      keyStorePassword = cfg.get(SPARK_SSL_KEYSTORE_PASSWORD)
    }

    val properties = cfg.getAllWithPrefix(PREFIX).toMap
    val filteredProperties = properties.filterKeys(key => {
      val prefixedKey = PREFIX + key
      prefixedKey != USERNAME &&
        prefixedKey != PASSWORD &&
        prefixedKey != CONNECTION_STRING &&
        prefixedKey != BUCKET_NAME &&
        prefixedKey != SCOPE_NAME &&
        prefixedKey != COLLECTION_NAME &&
        prefixedKey != WAIT_UNTIL_READY_TIMEOUT &&
        prefixedKey != SPARK_SSL_ENABLED &&
        prefixedKey != SPARK_SSL_KEYSTORE &&
        prefixedKey != SPARK_SSL_KEYSTORE_PASSWORD
    }).toSeq

    CouchbaseConfig(
      connectionString,
      credentials,
      bucketName,
      scopeName,
      collectionName,
      waitUntilReadyTimeout,
      SparkSslOptions(useSsl, keyStorePath, keyStorePassword),
      filteredProperties
    )
  }
}