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
package com.couchbase.spark

import java.lang.Long
import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.error.{CouchbaseOutOfMemoryException, TemporaryFailureException}
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection, RetryOptions}
import com.couchbase.spark.internal.OnceIterable
import org.apache.spark.rdd.RDD
import rx.functions.Action4
import rx.lang.scala.Observable
import rx.lang.scala.JavaConversions._

import scala.concurrent.duration.Duration

class DocumentRDDFunctions[D <: Document[_]](rdd: RDD[D])
  extends Serializable
  with Logging {

  private val cbConfig = CouchbaseConfig(rdd.context.getConf)

  def saveToCouchbase(bucketName: String = null, storeMode: StoreMode = StoreMode.UPSERT,
                      timeout: Option[Duration] = None): Unit = {
    val retryOpts = cbConfig.retryOpts


    rdd.foreachPartition(iter => {
      if (iter.nonEmpty) {
        val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()

        val kvTimeout = timeout
          .map(_.toMillis)
          .orElse(cbConfig.timeouts.kv)
          .getOrElse(bucket.environment().kvTimeout())

        Observable
          .from(OnceIterable(iter))
          .flatMap(doc =>  {
            storeMode match {
              case StoreMode.UPSERT =>
                maybeRetry(toScalaObservable(bucket.upsert[D](doc)
                  .timeout(kvTimeout, TimeUnit.MILLISECONDS)), retryOpts, doc.id())
              case StoreMode.INSERT_AND_FAIL =>
                maybeRetry(toScalaObservable(bucket.insert[D](doc)
                  .timeout(kvTimeout, TimeUnit.MILLISECONDS)), retryOpts, doc.id())
              case StoreMode.REPLACE_AND_FAIL =>
                maybeRetry(toScalaObservable(bucket.replace[D](doc)
                  .timeout(kvTimeout, TimeUnit.MILLISECONDS)), retryOpts, doc.id())
              case StoreMode.INSERT_AND_IGNORE =>
                maybeRetry(toScalaObservable(bucket.insert[D](doc)
                  .timeout(kvTimeout, TimeUnit.MILLISECONDS)), retryOpts, doc.id())
                .doOnError(err => logWarning("Insert failed, but suppressed.", err))
                .onErrorResumeNext(throwable => Observable.empty)
              case StoreMode.REPLACE_AND_IGNORE =>
                maybeRetry(toScalaObservable(bucket.replace[D](doc)
                  .timeout(kvTimeout, TimeUnit.MILLISECONDS)), retryOpts, doc.id())
                .doOnError(err => logWarning("Replace failed, but suppressed.", err))
                .onErrorResumeNext(throwable => Observable.empty)
            }
          })
          .toBlocking
          .lastOrElse(null)
      }
    })
  }

  def maybeRetry(input: Observable[D], options: RetryOptions, id: String): Observable[D] = {
    options match {
      case null => input
      case _ => input.retryWhen(
          RetryBuilder
              .anyOf(classOf[TemporaryFailureException], classOf[BackpressureException],
              classOf[CouchbaseOutOfMemoryException])
          .delay(Delay.exponential(TimeUnit.MILLISECONDS, options.maxDelay, options.minDelay))
          .max(options.maxTries)
          .doOnRetry(new Action4[Integer, Throwable, java.lang.Long, TimeUnit] {
            override def call(t1: Integer, t2: Throwable, t3: Long, t4: TimeUnit): Unit = {
              logInfo(s"Transparently Retrying Operation for ID: ${id}")
            }
          })
          .build()
        )
      }
  }

}
