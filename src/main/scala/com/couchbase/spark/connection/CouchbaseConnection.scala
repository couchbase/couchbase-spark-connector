/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark.connection

import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.{Cluster, Bucket, CouchbaseCluster}
import org.jboss.netty.util.internal.ConcurrentHashMap

class CouchbaseConnection extends Serializable {

  @transient var clusterRef: Option[Cluster] = None

  @transient var buckets = new ConcurrentHashMap[String, Bucket]()

  def cluster(cfg: CouchbaseConfig): Cluster = {
    this.synchronized {
      if (clusterRef.isEmpty) {
        clusterRef = Option(CouchbaseCluster.create(DefaultCouchbaseEnvironment.create(), cfg.hosts:_*))
      }
      clusterRef.get
    }
  }

  def bucket(bucketName: String = null, cfg: CouchbaseConfig): Bucket = {
    val bname = if (bucketName == null) {
      if (cfg.buckets.size != 1) {
        throw new IllegalStateException("The bucket name can only be inferred if there is exactly 1 bucket set on the config")
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

}

object CouchbaseConnection {

  lazy val connection = new CouchbaseConnection()

  def apply() = connection

}
