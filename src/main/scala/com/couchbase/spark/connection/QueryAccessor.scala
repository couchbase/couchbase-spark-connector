package com.couchbase.spark.connection

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.Logging
import com.couchbase.spark.internal.LazyIterator
import com.couchbase.spark.rdd.CouchbaseQueryRow
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable


class QueryAccessor(cbConfig: CouchbaseConfig, query: Seq[N1qlQuery], bucketName: String = null)
  extends Logging {

  def compute(): Iterator[CouchbaseQueryRow] = {
    if (query.isEmpty) {
      return Iterator[CouchbaseQueryRow]()
    }

    val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()

    val maxDelay = cbConfig.retryOpts.maxDelay
    val minDelay = cbConfig.retryOpts.minDelay
    val maxRetries = cbConfig.retryOpts.maxTries

    LazyIterator {
      Observable.from(query)
        .flatMap(vq => toScalaObservable(bucket.query(vq).retryWhen(
          RetryBuilder
            .anyOf(classOf[BackpressureException])
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, maxDelay, minDelay))
            .max(maxRetries)
            .build()
        )))
        .doOnNext(result => {
          toScalaObservable(result.errors()).subscribe(err => {
            logError(s"Couchbase N1QL Query $query failed with $err")
          })
        })
        .flatMap(_.rows())
        .map(row => CouchbaseQueryRow(row.value()))
        .toBlocking
        .toIterable
        .iterator
    }

  }

}
