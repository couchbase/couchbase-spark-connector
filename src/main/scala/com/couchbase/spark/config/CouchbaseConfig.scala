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

import com.couchbase.client.core.util.ConnectionString
import org.apache.spark.SparkConf

import java.util.Locale
import scala.collection.immutable.SortedMap

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
        "Either an implicitBucket needs to be configured, " +
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

  private val FILTERED_OUT: List[String] = "username" :: "password" :: "connectionString" :: "implicitBucket" :: "implicitScope" :: "implicitCollection" :: "waitUntilReadyTimeout" :: "ssl.enabled" :: "ssl.keyStore" :: "ssl.keyStorePassword" :: "ssl.insecure" :: Nil

  /** @param connectionIdentifier
    *   this needs to be the full original string, to allow dynamic connections with e.g. the same hostname but different
    *    credentials or settings
    * @param schema
    *   "couchbase", "couchbases"
    * @param hostnameAndPort
    *   localhost:4590 or just localhost
    * @param settings
    *   any properties to apply e.g. "spark.couchbase.timeout.kvTimeout=10s" or
    *   "com.couchbase.some.property=10s"
    */
  case class ConnectionIdentifierParsed(
      connectionIdentifier: String,
      schema: String,
      hosts: Seq[ConnectionString.UnresolvedSocket],
      username: Option[String],
      password: Option[String],
      settings: Seq[(String, String)]
  ) {
    def getSetting(key: String): Option[String] = {
      settings.find(v => v._1 == key).map(v => v._2)
    }

    def connectionString: String = schema + "://" + hosts
      .map(v => v.host + (if (v.port == 0) "" else ":" + v.port))
      .mkString(",")
  }

  // "couchbase://hostname"
  // "couchbase://hostname:port"
  // "couchbase://username:password@hostname"
  // "couchbase://username:password@hostname:port"
  // "couchbase://user@pw:myhosts?spark.couchbase.timeout.kvTimeout=10s"
  private def parseConnectionIdentifier(
      connectionIdentifier: String
  ): ConnectionIdentifierParsed = {
    val cs = ConnectionString.create(connectionIdentifier)
    import scala.collection.JavaConverters._
    ConnectionIdentifierParsed(connectionIdentifier,
      cs.scheme().name().toLowerCase,
      cs.hosts.asScala,
      if (cs.username() == null) None else Some(cs.username.split(":")(0)),
      if (cs.username() == null) None else Some(cs.username.split(":")(1)),
      cs.params.asScala.toSeq)
  }

  def apply(cfg: SparkConf, connectionIdentifierOrig: Option[String] = None): CouchbaseConfig = {
    val (dynamicConfig, connectionIdentifier) =
      if (
        connectionIdentifierOrig.isDefined
        && (connectionIdentifierOrig.get.startsWith("couchbase://") || connectionIdentifierOrig.get
          .startsWith("couchbases://"))
      ) {
        val p = parseConnectionIdentifier(connectionIdentifierOrig.get)

        (Some(p), Some(p.connectionIdentifier))
      } else {
        (None, connectionIdentifierOrig)
      }

    // For all options, look for them with this priority:
    // 1. If a dynamic connectionIdentifier e.g. "couchbase://user@pw:hostname?spark.couchbase.timeout.kvTimeout=10s" is specified, use the settings from that.
    // 2. Else if a static connectionIdentifier e.g. "test" is specified, look for "spark.couchbase.property:test" in the config.
    // 3. If the above haven't found anything, fallback to "spark.couchbase.property" in the config.
    //    This allows the user to specify e.g. an implicit bucket at the top-level config without having to specify it on every dynamic connectionIdentifier.

    val connectionString: String = dynamicConfig
      .map(dc => dc.connectionString)
      .orElse(cfg.getOption(ident(CONNECTION_STRING, connectionIdentifier)))
      .orElse(cfg.getOption(CONNECTION_STRING))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Required config property ${CONNECTION_STRING}${
            connectionIdentifierOrig match {
              case Some(value) => ":" + value
              case None => ""
            }} is not present"
        )
      )

    val username: String = dynamicConfig
      .flatMap(dc => dc.username)
      .orElse(cfg.getOption(ident(USERNAME, connectionIdentifier)))
      .orElse(cfg.getOption(USERNAME))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Required config property ${USERNAME} is not present"
        )
      )

    val password: String = dynamicConfig
      .flatMap(dc => dc.password)
      .orElse(cfg.getOption(ident(PASSWORD, connectionIdentifier)))
      .orElse(cfg.getOption(PASSWORD))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Required config property ${PASSWORD} is not present"
        )
      )

    val credentials = Credentials(username, password)

    val bucketName: Option[String] = dynamicConfig
      .flatMap(dc => dc.getSetting(BUCKET_NAME))
      .orElse(cfg.getOption(ident(BUCKET_NAME, connectionIdentifier)))
      .orElse(cfg.getOption(BUCKET_NAME))

    val scopeName: Option[String] = dynamicConfig
      .flatMap(dc => dc.getSetting(SCOPE_NAME))
      .orElse(cfg.getOption(ident(SCOPE_NAME, connectionIdentifier)))
      .orElse(cfg.getOption(SCOPE_NAME))

    val collectionName: Option[String] = dynamicConfig
      .flatMap(dc => dc.getSetting(COLLECTION_NAME))
      .orElse(cfg.getOption(ident(COLLECTION_NAME, connectionIdentifier)))
      .orElse(cfg.getOption(COLLECTION_NAME))

    val waitUntilReadyTimeout: Option[String] = dynamicConfig
      .flatMap(dc => dc.getSetting(WAIT_UNTIL_READY_TIMEOUT))
      .orElse(cfg.getOption(ident(WAIT_UNTIL_READY_TIMEOUT, connectionIdentifier)))
      .orElse(cfg.getOption(WAIT_UNTIL_READY_TIMEOUT))

    var useSsl                           = false
    var keyStorePath: Option[String]     = None
    var keyStorePassword: Option[String] = None

    // check for spark-related SSL settings
    val sslEnabled = dynamicConfig
      .flatMap(dc => dc.getSetting(SPARK_SSL_ENABLED))
      .orElse(cfg.getOption(ident(SPARK_SSL_ENABLED, connectionIdentifier)))
      .getOrElse("false")
      .toBoolean

    if (sslEnabled) {
      useSsl = true
      keyStorePath = cfg.getOption(ident(SPARK_SSL_KEYSTORE, connectionIdentifier))
      keyStorePassword = cfg.getOption(ident(SPARK_SSL_KEYSTORE_PASSWORD, connectionIdentifier))
    }

    val sslInsecure = dynamicConfig
      .flatMap(dc => dc.getSetting(SPARK_SSL_INSECURE))
      .orElse(cfg.getOption(ident(SPARK_SSL_INSECURE, connectionIdentifier)))
      .getOrElse("false")
      .toBoolean

    // Look for any properties that we'd want to pass down to the Cluster property loaders - e.g. "com.couchbase." ones.
    // - sort properties so any connectionIdentified property will follow any default property with the same key
    val properties = SortedMap[String, String]() ++ cfg.getAllWithPrefix(PREFIX)
    val filteredProperties = properties
      .filterKeys(k => {
        !filteredOut(k, connectionIdentifier)
      })
      .map(kv => {
        // remove the :connectionIdentifier suffix from the key if there is one
        val k = if (connectionIdentifier.isDefined && kv._1.endsWith(":" + connectionIdentifier.get)) kv._1.substring(0, kv._1.length - connectionIdentifier.get.length - 1) else kv._1
        (k, kv._2) // connectionIdentified property will win over default because it is last
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

  /** indicates if the key should be filtered out of filteredProperties
    * Only properties that are not in the FILTERED_OUT list (username, password etc)
    * and properties not belonging to this connectionId should be filted out.
    *
    * @param key
    *   the key-only part of the property name
    * @param identifier
    *   the connection identifier.
    * @return
    *   should the property be filtered out
    */
  private def filteredOut(key: String, connectionIdentifier: Option[String]): Boolean = {
    val cidIndex = key.indexOf(':');
    val propertyName = if (cidIndex != -1) key.substring(0, cidIndex) else key
    // filtered out regardless of no cid, same cid or different cid
    FILTERED_OUT.contains(propertyName) ||
    // key has prefix, but connectionIdentifier not specified
    (cidIndex != -1 && connectionIdentifier.isEmpty) ||
    // key does not have prefix, but connectionIdentifier specified
    (cidIndex == -1 && connectionIdentifier.isDefined) ||
    // key has prefix and connectionIdentifier specified, but they don't match
    (cidIndex != -1 && (connectionIdentifier.isDefined && !key.substring(cidIndex + 1).equals(connectionIdentifier.get)))
  }
}
