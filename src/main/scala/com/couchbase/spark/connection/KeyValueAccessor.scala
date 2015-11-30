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

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.error.{CouchbaseOutOfMemoryException, TemporaryFailureException}
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.internal.LazyIterator
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.reflect.ClassTag

class KeyValueAccessor[D <: Document[_]]
  (cbConfig: CouchbaseConfig, ids: Seq[String], bucketName: String = null)
  (implicit ct: ClassTag[D]) {

  def compute(): Iterator[D] = {
    if (ids.isEmpty) {
      return Iterator[D]()
    }

    val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()
    val castTo = ct.runtimeClass.asInstanceOf[Class[D]]

    val maxDelay = cbConfig.retryOpts.maxDelay
    val minDelay = cbConfig.retryOpts.minDelay
    val maxRetries = cbConfig.retryOpts.maxTries

    LazyIterator {
      Observable
        .from(ids)
        .flatMap(id => toScalaObservable(bucket.get(id, castTo).retryWhen(
          RetryBuilder
            .anyOf(classOf[TemporaryFailureException], classOf[BackpressureException],
              classOf[CouchbaseOutOfMemoryException])
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, maxDelay, minDelay))
            .max(maxRetries)
            .build())
        ))
        .toBlocking
        .toIterable
        .iterator
    }
  }

}
