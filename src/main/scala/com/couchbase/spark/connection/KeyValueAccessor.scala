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

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.error.{CouchbaseOutOfMemoryException, TemporaryFailureException}
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.internal.LazyIterator
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

class KeyValueAccessor[D <: Document[_]]
  (cbConfig: CouchbaseConfig, ids: Seq[String], bucketName: String = null,
   timeout: Option[Duration])
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

    val kvTimeout = timeout
      .map(_.toMillis)
      .orElse(cbConfig.timeouts.kv)
      .getOrElse(bucket.environment().kvTimeout())

    LazyIterator {
      Observable
        .from(ids)
        .flatMap(id => toScalaObservable(bucket.get(id, castTo)
          .timeout(kvTimeout, TimeUnit.MILLISECONDS)
          .retryWhen(
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
