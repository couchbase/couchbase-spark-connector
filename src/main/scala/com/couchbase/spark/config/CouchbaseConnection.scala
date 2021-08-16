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

import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.scala.{Bucket, Cluster, ClusterOptions, Collection, Scope}
import com.couchbase.client.scala.env.ClusterEnvironment

import scala.collection.mutable
import scala.concurrent.duration.DurationInt


class CouchbaseConnection extends Serializable {

  @transient var envRef: Option[ClusterEnvironment] = None

  @transient var clusterRef: Option[Cluster] = None

  @transient var bucketsRef: mutable.Map[String, Bucket] = mutable.HashMap()

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      Thread.currentThread().setName("couchbase-shutdown-in-progress")
      CouchbaseConnection().stop()
      Thread.currentThread().setName("couchbase-shutdown-complete")
    }
  })

  def cluster(cfg: CouchbaseConfig): Cluster = {
    this.synchronized {
      if (envRef.isEmpty) {
        val builder = ClusterEnvironment.builder
        envRef = Option(builder.build.get)
      }

      if (clusterRef.isEmpty) {
        clusterRef = Option(Cluster.connect(
          cfg.connectionString,
          ClusterOptions
            .create(cfg.credentials.username, cfg.credentials.password)
            .environment(envRef.get)
        ).get)

        clusterRef.get.waitUntilReady(1.minutes)
      }

      clusterRef.get
    }
  }

  def bucket(cfg: CouchbaseConfig, bucketName: Option[String]): Bucket = {
    val bname = this.bucketName(cfg, bucketName)
    this.synchronized {
      if (bucketsRef.contains(bname)) {
        return bucketsRef(bname)
      }
      val bucket = cluster(cfg).bucket(bname)
      bucketsRef.put(bname, bucket)
      bucket.waitUntilReady(1.minutes)
      bucket
    }
  }

  def scope(cfg: CouchbaseConfig, bucketName: Option[String], scopeName: Option[String]): Scope = {
    val name = this.scopeName(cfg, scopeName)

    if (name.equals(CollectionIdentifier.DEFAULT_SCOPE)) {
      bucket(cfg, bucketName).defaultScope
    } else {
      bucket(cfg, bucketName).scope(name)
    }
  }

  def collection(cfg: CouchbaseConfig, bucketName: Option[String], scopeName: Option[String],
                 collectionName: Option[String]): Collection = {
    val parsedScopeName = this.scopeName(cfg, scopeName)
    val parsedCollectionName = this.collectionName(cfg, collectionName)

    if (parsedScopeName.equals(CollectionIdentifier.DEFAULT_SCOPE)
      && parsedCollectionName.equals(CollectionIdentifier.DEFAULT_COLLECTION)) {
      bucket(cfg, bucketName).defaultCollection
    } else {
      scope(cfg, bucketName, scopeName).collection(parsedCollectionName)
    }

  }

  def stop(): Unit = {
    this.synchronized {
      if (clusterRef.isDefined) {
        clusterRef.get.disconnect()
      }
      if (envRef.isDefined) {
        envRef.get.shutdown()
      }
    }
  }

  def bucketName(cfg: CouchbaseConfig, name: Option[String]): String = {
    name.orElse(cfg.bucketName) match {
      case Some(name) => name
      case None => throw InvalidArgumentException
        .fromMessage("No bucketName provided (neither configured globally, "
          + "nor in the per-command options)")
    }
  }

  private def scopeName(cfg: CouchbaseConfig, name: Option[String]): String = {
    name.orElse(cfg.scopeName) match {
      case Some(name) => name
      case None => CollectionIdentifier.DEFAULT_SCOPE
    }
  }

  private def collectionName(cfg: CouchbaseConfig, name: Option[String]): String = {
    name.orElse(cfg.collectionName) match {
      case Some(name) => name
      case None => CollectionIdentifier.DEFAULT_COLLECTION
    }
  }

}

object CouchbaseConnection {

  lazy val connection = new CouchbaseConnection()

  def apply() = connection

}