package com.couchbase.spark.connection

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.client.java.view.SpatialViewQuery
import com.couchbase.spark.Logging
import com.couchbase.spark.internal.LazyIterator
import com.couchbase.spark.rdd.CouchbaseSpatialViewRow
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.concurrent.duration.Duration


class SpatialViewAccessor(cbConfig: CouchbaseConfig, spatialQuery: Seq[SpatialViewQuery],
  bucketName: String = null, timeout: Option[Duration])
  extends Logging {

  def compute(): Iterator[CouchbaseSpatialViewRow] = {
    if (spatialQuery.isEmpty) {
      return Iterator[CouchbaseSpatialViewRow]()
    }

    val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()

    val maxDelay = cbConfig.retryOpts.maxDelay
    val minDelay = cbConfig.retryOpts.minDelay
    val maxRetries = cbConfig.retryOpts.maxTries

    val viewTimeout = timeout
      .map(_.toMillis)
      .orElse(cbConfig.timeouts.view)
      .getOrElse(bucket.environment().viewTimeout())


    LazyIterator {
      Observable.from(spatialQuery)
        .flatMap(vq => toScalaObservable(bucket.query(vq)
          .timeout(viewTimeout, TimeUnit.MILLISECONDS)
          .retryWhen(
          RetryBuilder
            .anyOf(classOf[BackpressureException])
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, maxDelay, minDelay))
            .max(maxRetries)
            .build()
        )))
        .doOnNext(result => {
          toScalaObservable(result.error()).subscribe(err => {
            logError(s"Couchbase View Query $spatialQuery failed with $err")
          })
        })
        .flatMap(result => toScalaObservable(result.rows()))
        .map(row => CouchbaseSpatialViewRow(row.id(), row.key(), row.value(), row.geometry()))
        .toBlocking
        .toIterable
        .iterator
    }

  }

}
