package com.couchbase.spark.connection

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.analytics.AnalyticsQuery
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.Logging
import com.couchbase.spark.internal.LazyIterator
import com.couchbase.spark.rdd.CouchbaseQueryRow
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.concurrent.duration.Duration

case class CouchbaseAnalyticsRow(value: JsonObject)

class AnalyticsAccessor(cbConfig: CouchbaseConfig,
                        queries: Seq[AnalyticsQuery],
                        bucketName: String = null,
                        timeout: Option[Duration])
  extends Logging {

  def compute(): Iterator[CouchbaseAnalyticsRow] = {
    if (queries.isEmpty) {
      return Iterator[CouchbaseAnalyticsRow]()
    }

    val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()

    val maxDelay = cbConfig.retryOpts.maxDelay
    val minDelay = cbConfig.retryOpts.minDelay
    val maxRetries = cbConfig.retryOpts.maxTries

    val queryTimeout = timeout
      .map(_.toMillis)
      .orElse(cbConfig.timeouts.query)
      .getOrElse(bucket.environment().queryTimeout())

    LazyIterator {
      Observable.from(queries)
        .flatMap(vq => toScalaObservable(bucket.query(vq)
          .timeout(queryTimeout, TimeUnit.MILLISECONDS)
          .retryWhen(
          RetryBuilder
            .anyOf(classOf[BackpressureException])
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, maxDelay, minDelay))
            .max(maxRetries)
            .build()
        )))
        .doOnNext(result => {
          toScalaObservable(result.errors()).subscribe(err => {
            logError(s"Couchbase Analytics Queries $queries failed with $err")
          })
        })
        .flatMap(_.rows())
        .map(row => CouchbaseAnalyticsRow(row.value()))
        .toBlocking
        .toIterable
        .iterator
    }

  }

}
