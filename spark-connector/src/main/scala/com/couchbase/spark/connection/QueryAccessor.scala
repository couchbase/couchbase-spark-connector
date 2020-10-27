package com.couchbase.spark.connection

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.{BackpressureException, CouchbaseException}
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.error.QueryExecutionException
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.Logging
import com.couchbase.spark.internal.LazyIterator
import com.couchbase.spark.rdd.CouchbaseQueryRow
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.concurrent.duration.Duration


class QueryAccessor(cbConfig: CouchbaseConfig, query: Seq[N1qlQuery], bucketName: String = null,
                    timeout: Option[Duration])
  extends Logging {

  def compute(): Iterator[CouchbaseQueryRow] = {
    if (query.isEmpty) {
      return Iterator[CouchbaseQueryRow]()
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
      Observable.from(query)
        .flatMap(vq => toScalaObservable(bucket.query(vq)
          .timeout(queryTimeout, TimeUnit.MILLISECONDS)
          .retryWhen(
          RetryBuilder
            .anyOf(classOf[BackpressureException])
            .delay(Delay.exponential(TimeUnit.MILLISECONDS, maxDelay, minDelay))
            .max(maxRetries)
            .build()
        )))
        .flatMap(v => {
          val errors = toScalaObservable(v.errors())
            .map(e => {
              val msgRaw = e.getString("msg")
              val msg = if (msgRaw == null) "Query failure" else msgRaw
              throw new QueryExecutionException(msg, e)
            })

          v.rows().mergeWith(errors)
        })
        .map(row => CouchbaseQueryRow(row.value()))
        .toBlocking
        .toIterable
        .iterator
    }

  }

}
