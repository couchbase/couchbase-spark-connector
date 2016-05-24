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
package com.couchbase.spark.rdd

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.error.{CouchbaseOutOfMemoryException, TemporaryFailureException}
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.client.java.view.SpatialViewQuery
import com.couchbase.spark.connection.{CouchbaseConnection, CouchbaseConfig}
import com.couchbase.spark.internal.LazyIterator
import org.apache.spark.{Partition, TaskContext, SparkContext}
import org.apache.spark.rdd.RDD

import rx.lang.scala.JavaConversions._

case class CouchbaseSpatialViewRow(id: String, key: Any, value: Any, geometry: JsonObject)

class SpatialViewRDD
  (@transient sc: SparkContext, viewQuery: SpatialViewQuery, bucketName: String = null)
  extends RDD[CouchbaseSpatialViewRow](sc, Nil) {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext):
    Iterator[CouchbaseSpatialViewRow] = {
    val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()

    val maxDelay = cbConfig.retryOpts.maxDelay
    val minDelay = cbConfig.retryOpts.minDelay
    val maxRetries = cbConfig.retryOpts.maxTries

    LazyIterator {
      toScalaObservable(bucket.query(viewQuery).retryWhen(
        RetryBuilder
          .anyOf(classOf[BackpressureException])
          .delay(Delay.exponential(TimeUnit.MILLISECONDS, maxDelay, minDelay))
          .max(maxRetries)
          .build()
        ))
        .flatMap(result => toScalaObservable(result.rows()))
        .map(row => CouchbaseSpatialViewRow(row.id(), row.key(), row.value(), row.geometry()))
        .toBlocking
        .toIterable
        .iterator
    }
  }

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

}

object SpatialViewRDD {
  def apply(sc: SparkContext, bucketName: String, viewQuery: SpatialViewQuery) =
    new SpatialViewRDD(sc, viewQuery, bucketName)
}
