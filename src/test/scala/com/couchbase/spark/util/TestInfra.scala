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
import com.couchbase.client.scala.Cluster
import com.couchbase.client.scala.manager.bucket.CreateBucketSettings
import com.couchbase.client.scala.manager.collection.CreateCollectionSettings
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.kv.KeyValueOptions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, user}

import java.util
import java.util.{Arrays, UUID}


class TestInfraConnectedToSpark(
    private val customizer: Option[(SparkSession.Builder, Params) => Unit],
    val params: Params
) extends Logging {
  private val connectionString = params.connectionString

  logInfo(s"Creating Spark connection using Couchbase cluster ${connectionString}")

  private val sparkBuilder = SparkSession
    .builder()
    .master("local[*]")
    .appName(this.getClass.getSimpleName)

  customizer match {
    case Some(value) =>
      value(sparkBuilder, params)
    case None =>
      sparkBuilder
        .config("spark.couchbase.connectionString", params.connectionString)
        .config("spark.couchbase.username", params.username)
        .config("spark.couchbase.password", params.password)
        .config("spark.couchbase.implicitBucket", params.bucketName)

  }

  val spark: SparkSession = sparkBuilder.getOrCreate()

  def prepareAirportSampleData(): TestInfraConnectedToSpark = {
    val airports = spark.read
      .json("src/test/resources/airports.json")

    airports
      .withColumn("type", lit("airport"))
      .write
      .format("couchbase.kv")
      .mode(SaveMode.Overwrite)
      .save()

    airports.write
      .format("couchbase.kv")
      .option(KeyValueOptions.Scope, params.scopeName)
      .option(KeyValueOptions.Collection, params.collectionName)
      .mode(SaveMode.Overwrite)
      .save()

    this
  }

  def stop(): Unit = {
    logInfo(s"Stopping test and removing bucket ${params.bucketName}")
    params.cluster.buckets.dropBucket(params.bucketName).get
    CouchbaseConnection().stop()
    spark.stop()
    params.cluster.disconnect()
  }
}

case class Params(
    connectionString: String,
    username: String,
    password: String,
    private val _bucketName: Option[String],
    private val _scopeName: Option[String],
    private val _collectionName: Option[String],
    cluster: Cluster
) {
  def bucketName: String = {
    _bucketName match {
      case Some(value) => value
      case None =>
        throw new RuntimeException(
          "Test bug: test needs bucket name but the bucket has not been created"
        )
    }
  }

  def scopeName: String = {
    _scopeName match {
      case Some(value) => value
      case None =>
        throw new RuntimeException(
          "Test bug: test needs scope name but the scope has not been created"
        )
    }
  }

  def collectionName: String = {
    _collectionName match {
      case Some(value) => value
      case None =>
        throw new RuntimeException(
          "Test bug: test needs collection name but the collection has not been created"
        )
    }
  }
}

class TestInfraBuilder extends Logging {

  private val connectionString = "localhost"
  private val username         = "Administrator"
  private val password         = "password"

  private var bucketName: Option[String]     = None
  private var scopeName: Option[String]      = None
  private var collectionName: Option[String] = None
  // Cannot use the CouchbaseConnection() as that will do bucket.waitUntilReady(),
  // so we need to create the bucket first using a regular Scala SDK Cluster.
  private val cluster = Cluster.connect(connectionString, username, password).get

  def createBucket(bn: String): TestInfraBuilder = {
    this.bucketName match {
      case Some(_) => throw new RuntimeException("Test bug: have already created bucket")
      case None =>

        this.bucketName = Some(bn)

        logInfo(s"Creating bucket ${bn}")

        try {
          cluster.buckets.create(CreateBucketSettings(bn, 100)).get
        }
        catch {
          case _: BucketExistsException =>
        }
        ConsistencyUtil.waitUntilBucketPresent(cluster.async.core, bn)

        this
    }
  }

  def createScope(): TestInfraBuilder = {
    bucketName match {
      case Some(bn) =>
        scopeName match {
          case Some(_) => throw new RuntimeException("Test bug: have already created scope")
          case None =>
            val sn = UUID.randomUUID.toString.substring(0, 6)
            this.scopeName = Some(sn)

            logInfo(s"Creating scope ${bn}.${sn}")

            val bucket = cluster.bucket(bn)
            bucket.collections.createScope(sn).get
            ConsistencyUtil.waitUntilScopePresent(cluster.async.core, bn, sn)

            this
        }
      case None =>
        throw new RuntimeException("Test bug: trying to create scope before have created bucket")
    }
  }

  def createCollection() = {
    scopeName match {
      case Some(sn) =>
        collectionName match {
          case Some(_) => throw new RuntimeException("Test bug: have already created collection")
          case None =>
            val bn = bucketName.get

            val cn = UUID.randomUUID.toString.substring(0, 6)
            this.collectionName = Some(cn)

            logInfo(s"Creating collection ${bn}.${sn}.${cn}")

            val bucket = cluster.bucket(bn)
            bucket.collections.createCollection(sn, cn, CreateCollectionSettings()).get
            ConsistencyUtil.waitUntilCollectionPresent(cluster.async.core, bn, sn, cn)

            this
        }
      case None =>
        throw new RuntimeException(
          "Test bug: trying to create collection before have created scope"
        )
    }
  }

  def createBucketScopeAndCollection(bucketName: String) = {
    createBucket(bucketName)
    createScope()
    createCollection()
  }

  private def buildParams = {
    Params(connectionString, username, password, bucketName, scopeName, collectionName, cluster)
  }

  def connectToSpark(): TestInfraConnectedToSpark = {
    new TestInfraConnectedToSpark(None, buildParams)
  }

  def connectToSpark(
      customizer: (SparkSession.Builder, Params) => Unit
  ): TestInfraConnectedToSpark = {
    new TestInfraConnectedToSpark(Some(customizer), buildParams)
  }

}

object TestInfraBuilder {
  def apply(): TestInfraBuilder = new TestInfraBuilder()
}
