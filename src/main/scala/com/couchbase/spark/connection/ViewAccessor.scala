package com.couchbase.spark.connection

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.Logging
import com.couchbase.spark.internal.LazyIterator
import com.couchbase.spark.rdd.CouchbaseViewRow
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.concurrent.duration.Duration


class ViewAccessor(cbConfig: CouchbaseConfig, viewQuery: Seq[ViewQuery], bucketName: String = null,
                   timeout: Option[Duration])
  extends Logging {

  def compute(): Iterator[CouchbaseViewRow] = {
    if (viewQuery.isEmpty) {
      return Iterator[CouchbaseViewRow]()
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
      Observable.from(viewQuery)
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

}
