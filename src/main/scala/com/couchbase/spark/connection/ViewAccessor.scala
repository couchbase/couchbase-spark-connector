package com.couchbase.spark.connection

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.internal.LazyIterator
import com.couchbase.spark.rdd.CouchbaseViewRow
import org.apache.spark.Logging
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable


class ViewAccessor(cbConfig: CouchbaseConfig, viewQuery: Seq[ViewQuery], bucketName: String = null)
  extends Logging {

  def compute(): Iterator[CouchbaseViewRow] = {
    val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()

    val maxDelay = cbConfig.retryOpts.maxDelay
    val minDelay = cbConfig.retryOpts.minDelay
    val maxRetries = cbConfig.retryOpts.maxTries

    LazyIterator {
      Observable.from(viewQuery)
        .flatMap(vq => toScalaObservable(bucket.query(vq).retryWhen(
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
