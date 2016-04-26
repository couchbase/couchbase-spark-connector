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
package com.couchbase.spark

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.error.{CouchbaseOutOfMemoryException, TemporaryFailureException}
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection, RetryOptions}
import com.couchbase.spark.internal.OnceIterable
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

class DocumentRDDFunctions[D <: Document[_]](rdd: RDD[D])
  extends Serializable
    with Logging {

  private val cbConfig = CouchbaseConfig(rdd.context.getConf)

  def saveToCouchbase(bucketName: String = null,
                      storeMode: StoreMode = StoreMode.UPSERT,
                      retryOptions: RetryOptions = null): Unit = {
    rdd.foreachPartition(iter => {
      if (iter.nonEmpty) {
        val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()
        Observable
          .from(OnceIterable(iter))
          .flatMap(doc => {
            storeMode match {
              case StoreMode.UPSERT =>
                toScalaObservable(withRetry(bucket.upsert[D](doc), retryOptions))
              case StoreMode.INSERT_AND_FAIL =>
                toScalaObservable(withRetry(bucket.insert[D](doc), retryOptions))
              case StoreMode.REPLACE_AND_FAIL =>
                toScalaObservable(withRetry(bucket.replace[D](doc),retryOptions))
              case StoreMode.INSERT_AND_IGNORE =>
                toScalaObservable(withRetry(bucket.insert[D](doc), retryOptions))
                .doOnError(err => logWarning("Insert failed, but suppressed.", err))
                .onErrorResumeNext(throwable => Observable.empty)
              case StoreMode.REPLACE_AND_IGNORE =>
                toScalaObservable(withRetry(bucket.replace[D](doc), retryOptions))
                .doOnError(err => logWarning("Replace failed, but suppressed.", err))
                .onErrorResumeNext(throwable => Observable.empty)
            }
          })
          .toBlocking
          .last
      }
    })
  }

  def withRetry(observable: Observable[D], retryOptions: RetryOptions): Observable[D] = {
    retryOptions match {
      case null => observable
      case _ => observable.retryWhen(
        RetryBuilder
          .anyOf(classOf[TemporaryFailureException], classOf[BackpressureException],
            classOf[CouchbaseOutOfMemoryException])
          .delay(Delay.exponential(TimeUnit.MILLISECONDS, retryOptions.maxDelay, retryOptions.minDelay))
          .max(retryOptions.maxTries)
          .build())
    }
  }
}
