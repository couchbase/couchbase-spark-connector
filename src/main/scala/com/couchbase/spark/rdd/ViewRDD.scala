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
import com.couchbase.client.java.error.{CouchbaseOutOfMemoryException, TemporaryFailureException}
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.internal.LazyIterator
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import rx.lang.scala.JavaConversions._

case class CouchbaseViewRow(id: String, key: Any, value: Any)

class ViewRDD(@transient sc: SparkContext, viewQuery: ViewQuery, bucketName: String = null,
              deps: Seq[Dependency[_]])
  extends RDD[CouchbaseViewRow](sc, deps) {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseViewRow] = {
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
        .doOnNext(result => {
          toScalaObservable(result.error()).subscribe(err => {
            logError(s"Couchbase View Query $viewQuery failed with $err")
          })
        })
        .flatMap(result => toScalaObservable(result.rows()))
        .map(row => CouchbaseViewRow(row.id(), row.key(), row.value()))
        .toBlocking
        .toIterable
        .iterator
    }
  }

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

}

object ViewRDD {
  def apply(sc: SparkContext, bucketName: String, viewQuery: ViewQuery) =
    new ViewRDD(sc, viewQuery, bucketName, Nil)
}
