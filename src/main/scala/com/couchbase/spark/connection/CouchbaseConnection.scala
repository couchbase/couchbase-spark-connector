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

import com.couchbase.client.java.env.{CouchbaseEnvironment, DefaultCouchbaseEnvironment}
import com.couchbase.client.java.{Bucket, Cluster, CouchbaseCluster}
import com.couchbase.spark.Logging
import org.jboss.netty.util.internal.ConcurrentHashMap

class CouchbaseConnection extends Serializable with Logging {

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      Thread.currentThread().setName("couchbase-shutdown-in-progress")
      CouchbaseConnection().stop()
      Thread.currentThread().setName("couchbase-shutdown-complete")

    }
  })

  @transient var envRef: Option[CouchbaseEnvironment] = None

  @transient var clusterRef: Option[Cluster] = None

  @transient var buckets = new ConcurrentHashMap[String, Bucket]()

  def cluster(cfg: CouchbaseConfig): Cluster = {
    val currentProp = System.getProperty("com.couchbase.dcpEnabled")
    if (currentProp == null || currentProp.isEmpty) {
      System.setProperty("com.couchbase.dcpEnabled", "true")
    }

    this.synchronized {
      if (envRef.isEmpty) {
        envRef = Option(DefaultCouchbaseEnvironment.create())
      }
      if (clusterRef.isEmpty) {
        clusterRef = Option(CouchbaseCluster.create(envRef.get, cfg.hosts:_*))
      }
      clusterRef.get
    }
  }

  def bucket(cfg: CouchbaseConfig, bucketName: String = null): Bucket = {
    val bname = if (bucketName == null) {
      if (cfg.buckets.size != 1) {
        throw new IllegalStateException("The bucket name can only be inferred if there is "
          + "exactly 1 bucket set on the config")
      } else {
        cfg.buckets.head.name
      }
    } else {
      bucketName
    }
    this.synchronized {
      var bucket = buckets.get(bname)
      if (bucket != null) {
        return bucket
      }

      val foundBucketConfig = cfg.buckets.filter(_.name == bname)
      if (foundBucketConfig.isEmpty) {
        throw new IllegalStateException("Not able to find bucket password for bucket " + bname)
      }

      bucket = cluster(cfg).openBucket(bname, foundBucketConfig.head.password)
      buckets.put(bname, bucket)
      bucket
    }
  }

  def stop(): Unit = {
    this.synchronized {
      logInfo("Performing Couchbase SDK Shutdown")
      if (clusterRef.isDefined) {
        clusterRef.get.disconnect()
        clusterRef = None
      }
      if (envRef.isDefined) {
        envRef.get.shutdown()
        envRef = None
      }
    }
  }

}

object CouchbaseConnection {

  lazy val connection = new CouchbaseConnection()

  def apply() = connection

}
