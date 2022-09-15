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

import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.config.CouchbaseConfig.extractConf
import com.couchbase.spark.query.{QueryConfig, QueryOptions}
import org.apache.spark.SparkConf

import java.security.MessageDigest
import java.util.Locale
import java.util
import collection.JavaConverters._

case class Credentials(username: String, password: String)

case class SparkSslOptions(enabled: Boolean, keystorePath: Option[String], keystorePassword: Option[String], insecure: Boolean)
case class SparkOptions(connectionString: Option[String],
                        userName: Option[String],
                        password: Option[String],
                        credentials: Option[Credentials],
                        bucketName: Option[String],
                        scopeName: Option[String],
                        collectionName: Option[String],
                        waitUntilReadyTimeout: Option[String],
                        useSsl: Boolean,
                        keyStorePath: Option[String],
                        keyStorePassword: Option[String],
                        sslInsecure: Boolean,
                        sslConfigured: Boolean,
                        filteredProperties: Seq[(String,String)]
                       )

case class CouchbaseConfig(
  connectionString: String,
  credentials: Credentials,
  bucketName: Option[String],
  scopeName: Option[String],
  collectionName: Option[String],
  waitUntilReadyTimeout: Option[String],
  sparkSslOptions: SparkSslOptions,
  properties: Seq[(String, String)],
  queryConfig: QueryConfig
) {

  private var connectionKey: Option[String] = None

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

  def loadSparkOptions(conf: SparkConf) : CouchbaseConfig = {
    val sparkOptions = extractConf(conf)
    CouchbaseConfig(
      sparkOptions.connectionString.getOrElse(connectionString),
      sparkOptions.credentials.getOrElse(credentials),
      Option(sparkOptions.bucketName.getOrElse(bucketName.getOrElse(null))),
      Option(sparkOptions.scopeName.getOrElse(scopeName.getOrElse(null))),
      Option(sparkOptions.collectionName.getOrElse(collectionName.getOrElse(null))),
      Option(sparkOptions.waitUntilReadyTimeout.getOrElse(waitUntilReadyTimeout.getOrElse(null))),
      configureSSl(sparkOptions),
      mergeFilteredOptions(sparkOptions.filteredProperties),
      extractQueryConfig(conf)
    )
  }

  private def mergeFilteredOptions(filteredOptions:Seq[(String,String)]) : Seq[(String,String)] = {
    val mergedOptions = new util.HashMap[String,String]()
    properties.map(kv => mergedOptions.put(kv._1,kv._2))
    filteredOptions.map(kv => mergedOptions.put(kv._1,kv._2))
    mergedOptions.asScala.toSeq
  }

  private def configureSSl(sparkOptions: SparkOptions): SparkSslOptions = {
    if (sparkOptions.sslConfigured){
      SparkSslOptions(sparkOptions.useSsl, sparkOptions.keyStorePath, sparkOptions.keyStorePassword, sparkOptions.sslInsecure)
    } else{
      sparkSslOptions
    }
  }

  private def extractQueryConfig(properties: util.Map[String, String]): QueryConfig = {
    QueryConfig(
      implicitBucketNameOr(properties.get(QueryOptions.Bucket)),
      implicitScopeNameOr(properties.get(QueryOptions.Scope)),
      implicitCollectionName(properties.get(QueryOptions.Collection)),
      Option(properties.get(QueryOptions.IdFieldName)).getOrElse(DefaultConstants.DefaultIdFieldName),
      Option(properties.get(QueryOptions.Filter)),
      Option(properties.get(QueryOptions.ScanConsistency)).getOrElse(DefaultConstants.DefaultQueryScanConsistency),
      Option(properties.get(QueryOptions.Timeout)),
      Option(properties.get(QueryOptions.PushDownAggregate)).getOrElse("true").toBoolean
    )
  }

  def getKey: String = {
    if (!connectionKey.isDefined){
      connectionKey =  Some(MessageDigest.getInstance("MD5").digest(s"${connectionString}${credentials.username}${credentials.password}".getBytes()).toString)
    }
    connectionKey.get
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
  private val SPARK_SSL_INSECURE = SPARK_SSL_PREFIX + "insecure"

  private val EXTRA_SPARK_OPTIONS = s"${USERNAME},"

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

   val sparkOptions = extractConf(cfg)
    CouchbaseConfig(
      sparkOptions.connectionString.orNull,
      sparkOptions.credentials.orNull,
      sparkOptions.bucketName,
      sparkOptions.scopeName,
      sparkOptions.collectionName,
      sparkOptions.waitUntilReadyTimeout,
      SparkSslOptions(sparkOptions.useSsl, sparkOptions.keyStorePath, sparkOptions.keyStorePassword, sparkOptions.sslInsecure),
      sparkOptions.filteredProperties,
      null
    )
  }

  private def extractConf(cfg: SparkConf): SparkOptions = {
    val connectionString = cfg.getOption(CONNECTION_STRING)

    val username = cfg.getOption(USERNAME)
    val password = cfg.getOption(PASSWORD)
    val credentials = if (username.isDefined && password.isDefined){
      Some(Credentials(username.get, password.get))
    } else{
      None
    }

    val bucketName = cfg.getOption(BUCKET_NAME)
    val scopeName = cfg.getOption(SCOPE_NAME)
    val collectionName = cfg.getOption(COLLECTION_NAME)
    val waitUntilReadyTimeout = cfg.getOption(WAIT_UNTIL_READY_TIMEOUT)

    var useSsl = false
    var keyStorePath: Option[String] = None
    var keyStorePassword: Option[String] = None

    // check for spark-related SSL settings
    if (cfg.get(SPARK_SSL_ENABLED, "false").toBoolean) {
      useSsl = true
      keyStorePath = cfg.getOption(SPARK_SSL_KEYSTORE)
      keyStorePassword = cfg.getOption(SPARK_SSL_KEYSTORE_PASSWORD)
    }

    val sslConfigured = cfg.contains(SPARK_SSL_INSECURE)
    val sslInsecure = cfg.get(SPARK_SSL_INSECURE, "false").toBoolean

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

    SparkOptions(connectionString,
      username,
      password,
      credentials,
      bucketName,
      scopeName,
      collectionName,
      waitUntilReadyTimeout,
      useSsl,
      keyStorePath,
      keyStorePassword,
      sslInsecure,
      sslConfigured,
      filteredProperties
    )
  }
}