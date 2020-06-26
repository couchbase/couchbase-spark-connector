package com.couchbase.spark.connection

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.BackpressureException
import com.couchbase.client.core.time.Delay
import com.couchbase.client.java.error.{CouchbaseOutOfMemoryException, TemporaryFailureException}
import com.couchbase.client.java.subdoc.SubdocOptionsBuilder
import com.couchbase.client.java.util.retry.RetryBuilder
import com.couchbase.spark.internal.LazyIterator
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.JavaConversions.toJavaObservable
import rx.lang.scala.Observable

import scala.concurrent.duration.Duration


sealed trait SubdocMutationSpec { def id: String }
case class SubdocUpsert(override val id: String, path: String, value: Any,
                        createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocInsert(override val id: String, path: String, value: Any,
                        createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocReplace(override val id: String, path: String, value: Any)
  extends SubdocMutationSpec
case class SubdocRemove(override val id: String, path: String) extends SubdocMutationSpec
case class SubdocCounter(override val id: String, path: String, delta: Long,
                         createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocArrayAppend(override val id: String, path: String, value: Any,
                        createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocArrayPrepend(override val id: String, path: String, value: Any,
                             createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocArrayInsert(override val id: String, path: String, value: Any,
                              createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocArrayAddUnique(override val id: String, path: String, value: Any,
                             createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocArrayAppendAll(override val id: String, path: String, values: Seq[Any],
                                createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocArrayPrependAll(override val id: String, path: String, values: Seq[Any],
                                createParents: Boolean = true) extends SubdocMutationSpec
case class SubdocMutationResult(cas: Seq[(String, Long)])

class SubdocMutationAccessor(cbConfig: CouchbaseConfig, specs: Seq[SubdocMutationSpec],
                             bucketName: String = null, timeout: Option[Duration]) {

  def compute(): Iterator[SubdocMutationResult] = {
    if (specs.isEmpty) {
      return Iterator[SubdocMutationResult]()
    }

    val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()
    val maxDelay = cbConfig.retryOpts.maxDelay
    val minDelay = cbConfig.retryOpts.minDelay
    val maxRetries = cbConfig.retryOpts.maxTries

    val kvTimeout = timeout
      .map(_.toMillis)
      .orElse(cbConfig.timeouts.kv)
      .getOrElse(bucket.environment().kvTimeout())



    // 1: group specs by id
    val convertedSpecs = specs.groupBy(_.id).map(tuple => {
      val specs = tuple._2
      val builder = bucket.mutateIn(tuple._1)

      specs.foreach {
        case SubdocUpsert(_, path, fragment, createParents) => builder.upsert(path, fragment,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocInsert(_, path, fragment, createParents) => builder.insert(path, fragment,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocReplace(_, path, fragment) => builder.replace(path, fragment)
        case SubdocCounter(_, path, delta, createParents) => builder.counter(path, delta,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocArrayAppend(_, path, fragment, createParents) => builder.arrayAppend(path,
          fragment,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocArrayPrepend(_, path, fragment, createParents) => builder.arrayPrepend(path,
          fragment,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocArrayInsert(_, path, fragment, createParents) => builder.arrayInsert(path,
          fragment,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocArrayAddUnique(_, path, fragment, createParents) => builder.arrayAddUnique(path,
          fragment,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocArrayAppendAll(_, path, fragments, createParents) => builder.arrayAppendAll(path,
          fragments,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocArrayPrependAll(_, path, fragments, createParents) => builder.arrayPrependAll(
          path, fragments,
          SubdocOptionsBuilder.builder().createPath(createParents))
        case SubdocRemove(_, path) => builder.remove(path)
      }
      builder
    }).toList

    val retry = RetryBuilder
      .anyOf(classOf[TemporaryFailureException], classOf[BackpressureException],
        classOf[CouchbaseOutOfMemoryException])
      .delay(Delay.exponential(TimeUnit.MILLISECONDS, maxDelay, minDelay))
      .max(maxRetries)
      .build()

    LazyIterator {
      Observable
        .from(convertedSpecs)
        .flatMap(builder =>
          toScalaObservable(builder.execute().timeout(kvTimeout, TimeUnit.MILLISECONDS)
          ).retryWhen(input =>
            retry.call(toJavaObservable(input))
          ))
        .toList
        .toBlocking
        .toIterable
          .map(list => {
            SubdocMutationResult(list.map(df => (df.id(), df.cas())))
          })
        .iterator
    }
  }
}
