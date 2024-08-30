/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.spark.util

import com.couchbase.client.core.error.BucketExistsException
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.scala.env.{ClusterEnvironment, SecurityConfig}
import com.couchbase.client.scala.manager.bucket.CreateBucketSettings
import com.couchbase.client.scala.manager.collection.CreateCollectionSettings
import com.couchbase.client.scala.{Cluster, ClusterOptions}
import com.couchbase.spark.kv.KeyValueOptions
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.UUID

case class CreatedResources(bucketName: String, scopeName: String, collectionName: String)

/** For creating resources needed by many tests, such as buckets, collections, and basic test data.
  */
class TestResourceCreator(settings: CouchbaseClusterSettings) extends Logging {
  // We need an SDK to create resources with.
  // Cannot use the CouchbaseConnection() as that will do bucket.waitUntilReady(),
  private val env = if (settings.tlsEnabled) {
    ClusterEnvironment.builder
      .securityConfig(
        SecurityConfig().enableTls(true).trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)
      )
      .build
      .get
  } else {
    ClusterEnvironment.create
  }
  private val cluster = Cluster
    .connect(
      settings.connectionString,
      ClusterOptions.create(settings.username, settings.password).environment(env)
    )
    .get

  def stop(): Unit = {
    cluster.disconnect()
    env.shutdown()
  }

  def prepareAirportSampleData(
      spark: SparkSession,
      createdResources: CreatedResources
  ): Unit = {
    prepareAirportSampleData(
      spark,
      createdResources.bucketName,
      createdResources.scopeName,
      createdResources.collectionName
    )
  }

  def prepareAirportSampleData(
      spark: SparkSession,
      bucketName: String,
      scopeName: String,
      collectionName: String
  ): Unit = {
    val airports = spark.read
      .json("src/test/resources/airports.json")

    // Write to both the default and a named collection, as tests are using both.
    airports
      // Because many tests are depending on filtering for this.
      .withColumn("type", lit("airport"))
      .write
      .format("couchbase.kv")
      .mode(SaveMode.Overwrite)
      .option(KeyValueOptions.Bucket, bucketName)
      .save()

    airports.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Bucket, bucketName)
      .option(KeyValueOptions.Scope, scopeName)
      .option(KeyValueOptions.Collection, collectionName)
      .mode(SaveMode.Overwrite)
      .save()
  }

  def createBucket(bucketName: Option[String] = None): String = {
    val bn = bucketName.getOrElse(UUID.randomUUID.toString.substring(0, 6))
    logInfo(s"Creating bucket ${bn}")

    try {
      cluster.buckets.create(CreateBucketSettings(bn, 100)).get
    } catch {
      case _: BucketExistsException =>
    }
    ConsistencyUtil.waitUntilBucketPresent(cluster.async.core, bn)

    bn
  }

  def createScope(bn: String, scopeName: Option[String] = None): String = {
    val sn = scopeName.getOrElse(UUID.randomUUID.toString.substring(0, 6))

    logInfo(s"Creating scope ${bn}.${sn}")

    val bucket = cluster.bucket(bn)
    bucket.collections.createScope(sn).get
    ConsistencyUtil.waitUntilScopePresent(cluster.async.core, bn, sn)

    sn
  }

  def createCollection(bn: String, sn: String, collectionName: Option[String] = None): String = {
    val cn = collectionName.getOrElse(UUID.randomUUID.toString.substring(0, 6))

    logInfo(s"Creating collection ${bn}.${sn}.${cn}")

    val bucket = cluster.bucket(bn)
    bucket.collections.createCollection(sn, cn, CreateCollectionSettings()).get
    ConsistencyUtil.waitUntilCollectionPresent(cluster.async.core, bn, sn, cn)

    cn
  }

  def createBucketScopeAndCollection(
      bucketName: Option[String] = None,
      scopeName: Option[String] = None,
      collectionName: Option[String] = None
  ): CreatedResources = {
    val bn = createBucket(bucketName)
    val sn = createScope(bn, scopeName)
    val cn = createCollection(bn, sn, collectionName)

    CreatedResources(bn, sn, cn)
  }

  def deleteBucket(bucketName: String): Unit = {
    logInfo(s"Deleting bucket ${bucketName}")
    cluster.buckets.dropBucket(bucketName)
  }

  def deleteScope(bucketName: String, scopeName: String): Unit = {
    logInfo(s"Deleting scope ${bucketName}.${scopeName}")
    val bucket = cluster.bucket(bucketName)
    bucket.collections.dropScope(scopeName)
  }

  def deleteCollection(bucketName: String, scopeName: String, collectionName: String): Unit = {
    logInfo(s"Deleting collection ${bucketName}.${scopeName}.${collectionName}")
    val bucket = cluster.bucket(bucketName)
    bucket.collections.dropCollection(scopeName, collectionName)
  }
}
