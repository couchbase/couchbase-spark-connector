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

case class SparkSslOptions(
    enabled: Boolean,
    keystorePath: Option[String],
    keystorePassword: Option[String],
    insecure: Boolean
)

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
      throw new IllegalArgumentException(
        "Either a implicitBucket needs to be configured, " +
          "or a bucket name needs to be provided as part of the options!"
      )
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

    if (
      lowerCasedConnstr.startsWith("couchbase://") || lowerCasedConnstr.startsWith("couchbases://")
    ) {
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

  private val PREFIX                   = SPARK_PREFIX + "couchbase."
  private val USERNAME                 = PREFIX + "username"
  private val PASSWORD                 = PREFIX + "password"
  private val CONNECTION_STRING        = PREFIX + "connectionString"
  private val BUCKET_NAME              = PREFIX + "implicitBucket"
  private val SCOPE_NAME               = PREFIX + "implicitScope"
  private val COLLECTION_NAME          = PREFIX + "implicitCollection"
  private val WAIT_UNTIL_READY_TIMEOUT = PREFIX + "waitUntilReadyTimeout"

  private val SPARK_SSL_PREFIX            = SPARK_PREFIX + "ssl."
  private val SPARK_SSL_ENABLED           = SPARK_SSL_PREFIX + "enabled"
  private val SPARK_SSL_KEYSTORE          = SPARK_SSL_PREFIX + "keyStore"
  private val SPARK_SSL_KEYSTORE_PASSWORD = SPARK_SSL_PREFIX + "keyStorePassword"
  private val SPARK_SSL_INSECURE          = SPARK_SSL_PREFIX + "insecure"

  def checkRequiredProperties(
      cfg: SparkConf,
      connectionIdentifier: Option[String] = None
  ): Unit = {
    if (!cfg.contains(ident(CONNECTION_STRING, connectionIdentifier))) {
      throw new IllegalArgumentException(
        "Required config property " + ident(
          CONNECTION_STRING,
          connectionIdentifier
        ) + " is not present"
      )
    }
    if (!cfg.contains(ident(USERNAME, connectionIdentifier))) {
      throw new IllegalArgumentException(
        "Required config property " + ident(USERNAME, connectionIdentifier) + " is not present"
      )
    }
    if (!cfg.contains(ident(PASSWORD, connectionIdentifier))) {
      throw new IllegalArgumentException(
        "Required config property " + ident(PASSWORD, connectionIdentifier) + " is not present"
      )
    }
  }

  def apply(cfg: SparkConf, connectionIdentifier: Option[String] = None): CouchbaseConfig = {
    checkRequiredProperties(cfg, connectionIdentifier)

    val connectionString = cfg.get(ident(CONNECTION_STRING, connectionIdentifier))

    val username    = cfg.get(ident(USERNAME, connectionIdentifier))
    val password    = cfg.get(ident(PASSWORD, connectionIdentifier))
    val credentials = Credentials(username, password)

    val bucketName     = cfg.getOption(ident(BUCKET_NAME, connectionIdentifier))
    val scopeName      = cfg.getOption(ident(SCOPE_NAME, connectionIdentifier))
    val collectionName = cfg.getOption(ident(COLLECTION_NAME, connectionIdentifier))
    val waitUntilReadyTimeout =
      cfg.getOption(ident(WAIT_UNTIL_READY_TIMEOUT, connectionIdentifier))

    var useSsl                           = false
    var keyStorePath: Option[String]     = None
    var keyStorePassword: Option[String] = None

    // check for spark-related SSL settings
    if (cfg.get(ident(SPARK_SSL_ENABLED, connectionIdentifier), "false").toBoolean) {
      useSsl = true
      keyStorePath = cfg.getOption(ident(SPARK_SSL_KEYSTORE, connectionIdentifier))
      keyStorePassword = cfg.getOption(ident(SPARK_SSL_KEYSTORE_PASSWORD, connectionIdentifier))
    }

    val sslInsecure = cfg.get(ident(SPARK_SSL_INSECURE, connectionIdentifier), "false").toBoolean

    val properties = cfg.getAllWithPrefix(PREFIX).toMap
    val filteredProperties = properties
      .filterKeys(key => {
        val prefixedKey = PREFIX + key
        prefixedKey != ident(USERNAME, connectionIdentifier) &&
        prefixedKey != ident(PASSWORD, connectionIdentifier) &&
        prefixedKey != ident(CONNECTION_STRING, connectionIdentifier) &&
        prefixedKey != ident(BUCKET_NAME, connectionIdentifier) &&
        prefixedKey != ident(SCOPE_NAME, connectionIdentifier) &&
        prefixedKey != ident(COLLECTION_NAME, connectionIdentifier) &&
        prefixedKey != ident(WAIT_UNTIL_READY_TIMEOUT, connectionIdentifier) &&
        prefixedKey != ident(SPARK_SSL_ENABLED, connectionIdentifier) &&
        prefixedKey != ident(SPARK_SSL_KEYSTORE, connectionIdentifier) &&
        prefixedKey != ident(SPARK_SSL_KEYSTORE_PASSWORD, connectionIdentifier)
      })
      .map(kv => {
        (kv._1, kv._2)
      })
      .toSeq

    CouchbaseConfig(
      connectionString,
      credentials,
      bucketName,
      scopeName,
      collectionName,
      waitUntilReadyTimeout,
      SparkSslOptions(useSsl, keyStorePath, keyStorePassword, sslInsecure),
      filteredProperties
    )
  }

  /** Adds the connection identifier suffix if present.
    *
    * @param property
    *   the property to suffix potentially.
    * @param identifier
    *   the connection identifier.
    * @return
    *   the suffixed property.
    */
  private def ident(property: String, identifier: Option[String]): String = {
    identifier match {
      case Some(i) => property + ":" + i
      case None    => property
    }
  }
}
