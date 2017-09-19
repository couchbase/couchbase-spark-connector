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
import com.couchbase.client.java.error.{CouchbaseOutOfMemoryException, TemporaryFailureException}
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.internal.LazyIterator
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.collection.mutable
import scala.concurrent.duration.Duration

case class SubdocLookupSpec(id: String, get: Seq[String], exists: Seq[String])

case class SubdocLookupResult(id: String, cas: Long, content: Map[String, Any],
                        exists: Map[String, Boolean])

class SubdocLookupAccessor(cbConfig: CouchbaseConfig, specs: Seq[SubdocLookupSpec],
                          bucketName: String = null, timeout: Option[Duration]) {

  def compute(): Iterator[SubdocLookupResult] = {
    if (specs.isEmpty) {
      return Iterator[SubdocLookupResult]()
    }

    val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()
    val maxDelay = cbConfig.retryOpts.maxDelay
    val minDelay = cbConfig.retryOpts.minDelay
    val maxRetries = cbConfig.retryOpts.maxTries

    val kvTimeout = timeout
      .map(_.toMillis)
      .orElse(cbConfig.timeouts.kv)
      .getOrElse(bucket.environment().kvTimeout())


    LazyIterator {
      Observable
        .from(specs)
        .flatMap(spec => {
            var builder = bucket.lookupIn(spec.id)
            spec.exists.foreach(builder.exists(_))
            spec.get.foreach(builder.get(_))
            toScalaObservable(builder.execute().timeout(kvTimeout, TimeUnit.MILLISECONDS)
            ).map(fragment => {
              val content = mutable.Map[String, Any]()
              spec.get.foreach(path => content.put(path, fragment.content(path)))
              val exists = mutable.Map[String, Boolean]()
              spec.exists.foreach(path =>  exists.put(path, fragment.status(path).isSuccess))
              SubdocLookupResult(spec.id, fragment.cas(), content.toMap, exists.toMap)
            }).retryWhen(
            RetryBuilder
              .anyOf(classOf[TemporaryFailureException], classOf[BackpressureException],
                classOf[CouchbaseOutOfMemoryException])
              .delay(Delay.exponential(TimeUnit.MILLISECONDS, maxDelay, minDelay))
              .max(maxRetries)
              .build())
        })
        .toBlocking
        .toIterable
        .iterator
    }
  }

}
