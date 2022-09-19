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
import com.couchbase.spark.config.CouchbaseConfig.{CONNECTION_STRING, PASSWORD, USERNAME, extractConf}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.security.MessageDigest
import java.util.Locale
import java.util
import collection.JavaConverters._

case class Credentials(username: String, password: String)

case class SparkSslOptions(enabled: Boolean, keystorePath: Option[String], keystorePassword: Option[String], insecure: Boolean)
case class DSOptions(connectionString: Option[String],
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
                            dsConfig: DSConfig
) {

  private var connectionKey: Option[Int] = None

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

  def loadDSOptions(options: util.Map[String,String]) : CouchbaseConfig = {
    val dsOptions = extractConf(options)
    val couchBaseConfig = CouchbaseConfig(
      dsOptions.connectionString.getOrElse(connectionString),
      dsOptions.credentials.getOrElse(credentials),
      Option(dsOptions.bucketName.getOrElse(bucketName.getOrElse(null))),
      Option(dsOptions.scopeName.getOrElse(scopeName.getOrElse(null))),
      Option(dsOptions.collectionName.getOrElse(collectionName.getOrElse(null))),
      Option(dsOptions.waitUntilReadyTimeout.getOrElse(waitUntilReadyTimeout.getOrElse(null))),
      configureSSl(dsOptions),
      mergeFilteredOptions(dsOptions.filteredProperties),
      extractDSConfig(options)
    )
    validateReqProps(couchBaseConfig,options)
    couchBaseConfig
  }

  private def mergeFilteredOptions(filteredOptions:Seq[(String,String)]) : Seq[(String,String)] = {
    val mergedOptions = new util.HashMap[String,String]()
    properties.map(kv => mergedOptions.put(kv._1,kv._2))
    filteredOptions.map(kv => mergedOptions.put(kv._1,kv._2))
    mergedOptions.asScala.toSeq
  }

  private def configureSSl(sparkOptions: DSOptions): SparkSslOptions = {
    if (sparkOptions.sslConfigured){
      SparkSslOptions(sparkOptions.useSsl, sparkOptions.keyStorePath, sparkOptions.keyStorePassword, sparkOptions.sslInsecure)
    } else{
      sparkSslOptions
    }
  }

  private def extractDSConfig(options: util.Map[String,String]): DSConfig = {
    DSConfig(
      implicitBucketNameOr(options.get(DSConfigOptions.Bucket)),
      implicitScopeNameOr(options.get(DSConfigOptions.Scope)),
      implicitCollectionName(options.get(DSConfigOptions.Collection)),
      Option(options.get(DSConfigOptions.IdFieldName)).getOrElse(DefaultConstants.DefaultIdFieldName),
      Option(options.get(DSConfigOptions.Filter)),
      Option(options.get(DSConfigOptions.ScanConsistency)).getOrElse(DefaultConstants.DefaultQueryScanConsistency),
      Option(options.get(DSConfigOptions.Timeout)),
      Option(options.get(DSConfigOptions.PushDownAggregate)).getOrElse("true").toBoolean,
      Option(options.get(DSConfigOptions.Durability)),
      Option(options.get(DSConfigOptions.Dataset))
    )
  }

  private def validateReqProps(couchbaseConfig: CouchbaseConfig,options: util.Map[String,String]): Unit = {
    if (couchbaseConfig.connectionString == null) {
      throw new IllegalArgumentException("Required config property " + CONNECTION_STRING + " is not present")
    }

    if (couchbaseConfig.credentials == null){
      if (!options.contains(DSConfigOptions.Username)) {
        throw new IllegalArgumentException("Required config property " + USERNAME + " is not present")
      }
      if (!options.contains(DSConfigOptions.Password)) {
        throw new IllegalArgumentException("Required config property " + PASSWORD + " is not present")
      }
    }
  }

  def getKey: Int = {
    if (!connectionKey.isDefined){
      connectionKey=Some(s"${connectionString}${credentials.username}${credentials.password}".hashCode)
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

  def checkRequiredProperties(options: util.Map[String,String]): Unit = {
    if (!options.contains(CONNECTION_STRING)) {
      throw new IllegalArgumentException("Required config property " + DSConfigOptions.ConnectionString + " is not present")
    }
    if (!options.contains(USERNAME)) {
      throw new IllegalArgumentException("Required config property " + DSConfigOptions.Username + " is not present")
    }
    if (!options.contains(PASSWORD)) {
      throw new IllegalArgumentException("Required config property " + DSConfigOptions.Password + " is not present")
    }
  }

  def apply(options: util.Map[String,String], chckReqProp: Boolean): CouchbaseConfig = {

    if(chckReqProp){
      checkRequiredProperties(options)
    }

   val sparkOptions = extractConf(options)
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

  private def extractConf(options: util.Map[String,String]): DSOptions = {
    val connectionString = Option(Option(options.get(CONNECTION_STRING)).getOrElse(Option(options.get(DSConfigOptions.ConnectionString)).getOrElse(null)))

    val username = Option(Option(options.get(USERNAME)).getOrElse(Option(options.get(DSConfigOptions.Username)).getOrElse(null)))
    val password = Option(Option(options.get(PASSWORD)).getOrElse(Option(options.get(DSConfigOptions.Password)).getOrElse(null)))
    val credentials = if (username.isDefined && password.isDefined){
      Some(Credentials(username.get, password.get))
    } else{
      None
    }

    val bucketName = Option(Option(options.get(BUCKET_NAME)).getOrElse(Option(options.get(DSConfigOptions.Bucket)).getOrElse(null)))
    val scopeName = Option(Option(options.get(SCOPE_NAME)).getOrElse(Option(options.get(DSConfigOptions.Scope)).getOrElse(null)))
    val collectionName = Option(Option(options.get(COLLECTION_NAME)).getOrElse(Option(options.get(DSConfigOptions.Collection)).getOrElse(null)))
    val waitUntilReadyTimeout = Option(Option(options.get(WAIT_UNTIL_READY_TIMEOUT)).getOrElse(Option(options.get(DSConfigOptions.WaitUntilReadyTimeout)).getOrElse(null)))

    var useSsl = false
    var keyStorePath: Option[String] = None
    var keyStorePassword: Option[String] = None

    // check for spark-related SSL settings
    if (Option(options.get(SPARK_SSL_ENABLED)).getOrElse("false").toBoolean) {
      useSsl = true
      keyStorePath = Option(options.get(SPARK_SSL_KEYSTORE))
      keyStorePassword = Option(options.get(SPARK_SSL_KEYSTORE_PASSWORD))
    }

    val sslConfigured = options.contains(SPARK_SSL_INSECURE)
    val sslInsecure = Option(options.get(SPARK_SSL_INSECURE)).getOrElse("false").toBoolean

    val properties = options.getAllWithPrefix(PREFIX).toMap
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
        prefixedKey != SPARK_SSL_KEYSTORE_PASSWORD &&
        key != DSConfigOptions.ConnectionString &&
        key != DSConfigOptions.Username &&
        key != DSConfigOptions.Password &&
        key != DSConfigOptions.Bucket &&
        key != DSConfigOptions.Scope &&
        key != DSConfigOptions.Collection &&
        key != DSConfigOptions.WaitUntilReadyTimeout
    }).toSeq

    DSOptions(connectionString,
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