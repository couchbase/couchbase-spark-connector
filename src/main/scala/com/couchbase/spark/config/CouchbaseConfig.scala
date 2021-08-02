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

case class Credentials(username: String, password: String)

case class CouchbaseConfig(
  connectionString: String,
  credentials: Credentials,
  bucketName: Option[String],
  scopeName: Option[String],
  collectionName: Option[String]
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

}

object CouchbaseConfig {

  private val PREFIX = "spark.couchbase."
  private val USERNAME = PREFIX + "username"
  private val PASSWORD = PREFIX + "password"
  private val CONNECTION_STRING = PREFIX + "connectionString"
  private val BUCKET_NAME = PREFIX + "implicitBucket"
  private val SCOPE_NAME = PREFIX + "implicitScope"
  private val COLLECTION_NAME = PREFIX + "implicitCollection"

  def apply(cfg: SparkConf): CouchbaseConfig = {
    val connectionString = cfg.get(CONNECTION_STRING)

    val username = cfg.get(USERNAME)
    val password = cfg.get(PASSWORD)
    val credentials = Credentials(username, password)

    val bucketName = cfg.getOption(BUCKET_NAME)
    val scopeName = cfg.getOption(SCOPE_NAME)
    val collectionName = cfg.getOption(COLLECTION_NAME)

    CouchbaseConfig(connectionString, credentials, bucketName, scopeName, collectionName)
  }
}