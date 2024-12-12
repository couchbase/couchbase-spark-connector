/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.spark.kv

import com.couchbase.client.core.error.{DocumentExistsException, DocumentNotFoundException}
import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.kv.{
  InsertOptions,
  MutateInOptions,
  MutateInResult,
  MutationResult,
  RemoveOptions,
  ReplaceOptions,
  UpsertOptions
}
import com.couchbase.spark.{DefaultConstants, Keyspace}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import reactor.core.scala.publisher.{SFlux, SMono}

import java.util.UUID
import scala.concurrent.duration.NANOSECONDS

object KeyValueOperationRunner extends Logging {

  def upsert[T](
      config: CouchbaseConfig,
      keyspace: Keyspace,
      upsert: Seq[Upsert[T]],
      upsertOptions: UpsertOptions = null,
      connectionIdentifier: Option[String]
  )(implicit serializer: JsonSerializer[T]): Seq[MutationResult] = {
    val connection = CouchbaseConnection(connectionIdentifier)
    val cluster    = connection.cluster(config)
    val start      = System.nanoTime()
    val opUUID     = UUID.randomUUID().toString.substring(0, 6)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull)
      .getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (upsertOptions == null) {
      UpsertOptions()
    } else {
      upsertOptions
    }

    logInfo(
      s"Performing bulk KV upsert op ${opUUID} against ${upsert.length} ids with options $options.  " +
        s"See TRACE logging for all ids"
    )
    logTrace(
      s"Performing bulk upsert op ${opUUID} against ids ${upsert.map(_.id)} with options $options"
    )

    val out = SFlux
      .fromIterable(upsert)
      .flatMap(doc => collection.upsert(doc.id, doc.content, options))
      .collectSeq()
      .block()

    logInfo(
      s"Completed bulk KV upsert op ${opUUID} against ${upsert.length} ids in ${NANOSECONDS
          .toMillis(System.nanoTime - start)}ms"
    )

    out
  }

  def insert[T](
      config: CouchbaseConfig,
      keyspace: Keyspace,
      insert: Seq[Insert[T]],
      insertOptions: InsertOptions = null,
      ignoreIfExists: Boolean = false,
      connectionIdentifier: Option[String]
  )(implicit serializer: JsonSerializer[T]): Seq[MutationResult] = {
    val connection = CouchbaseConnection(connectionIdentifier)
    val cluster    = connection.cluster(config)
    val start      = System.nanoTime()
    val opUUID     = UUID.randomUUID().toString.substring(0, 6)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull)
      .getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (insertOptions == null) {
      InsertOptions()
    } else {
      insertOptions
    }

    logInfo(
      s"Performing bulk KV insert op ${opUUID} against ${insert.length} ids with options $options and " +
        s"ignoreIfExists $ignoreIfExists.  See TRACE logging for all ids"
    )
    logTrace(
      s"Performing bulk KV insert op ${opUUID} against ids ${insert.map(_.id)} with options $options and " +
        s"ignoreIfExists $ignoreIfExists"
    )

    val out = SFlux
      .fromIterable(insert)
      .flatMap(doc => {
        collection
          .insert(doc.id, doc.content, options)
          .onErrorResume(t => {
            if (ignoreIfExists && t.isInstanceOf[DocumentExistsException]) {
              SMono.empty
            } else {
              SMono.error(t)
            }
          })
      })
      .collectSeq()
      .block()

    logInfo(
      s"Completed bulk KV insert op ${opUUID} against ${insert.length} ids in ${NANOSECONDS
          .toMillis(System.nanoTime - start)}ms"
    )

    out
  }

  def replace[T](
      config: CouchbaseConfig,
      keyspace: Keyspace,
      replace: Seq[Replace[T]],
      replaceOptions: ReplaceOptions = null,
      ignoreIfNotFound: Boolean = false,
      connectionIdentifier: Option[String]
  )(implicit serializer: JsonSerializer[T]): Seq[MutationResult] = {
    val connection = CouchbaseConnection(connectionIdentifier)
    val cluster    = connection.cluster(config)
    val start      = System.nanoTime()
    val opUUID     = UUID.randomUUID().toString.substring(0, 6)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull)
      .getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (replaceOptions == null) {
      ReplaceOptions()
    } else {
      replaceOptions
    }

    logInfo(
      s"Performing bulk KV replace against ${replace.length} ids with options $options and " +
        s"ignoreIfExists $ignoreIfNotFound.  See TRACE logging for all ids"
    )
    logTrace(
      s"Performing bulk KV replace against ids ${replace.map(_.id)} with options $options and " +
        s"ignoreIfNotFound $ignoreIfNotFound"
    )

    val out = SFlux
      .fromIterable(replace)
      .flatMap(doc => {
        collection
          .replace(doc.id, doc.content, options.cas(doc.cas))
          .onErrorResume(t => {
            if (ignoreIfNotFound && t.isInstanceOf[DocumentNotFoundException]) {
              SMono.empty
            } else {
              SMono.error(t)
            }
          })
      })
      .collectSeq()
      .block()

    logInfo(
      s"Completed bulk KV replace op ${opUUID} against ${replace.length} ids in ${NANOSECONDS
          .toMillis(System.nanoTime - start)}ms"
    )

    out
  }

  def remove(
      config: CouchbaseConfig,
      keyspace: Keyspace,
      remove: Seq[Remove],
      removeOptions: RemoveOptions = null,
      ignoreIfNotFound: Boolean = false,
      connectionIdentifier: Option[String]
  ): Seq[MutationResult] = {
    val connection = CouchbaseConnection(connectionIdentifier)
    val cluster    = connection.cluster(config)
    val start      = System.nanoTime()
    val opUUID     = UUID.randomUUID().toString.substring(0, 6)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull)
      .getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (removeOptions == null) {
      RemoveOptions()
    } else {
      removeOptions
    }

    logInfo(
      s"Performing bulk KV remove against ${remove.length} ids with options $options " +
        s"and ignoreIfNotFound $ignoreIfNotFound.  See TRACE logging for all ids"
    )
    logTrace(
      s"Performing bulk KV remove against ids ${remove.map(_.id)} with options $options " +
        s"and ignoreIfNotFound $ignoreIfNotFound"
    )

    val out = SFlux
      .fromIterable(remove)
      .flatMap(doc => {
        collection
          .remove(doc.id, options.cas(doc.cas))
          .onErrorResume(t => {
            if (ignoreIfNotFound && t.isInstanceOf[DocumentNotFoundException]) {
              SMono.empty
            } else {
              SMono.error(t)
            }
          })
      })
      .collectSeq()
      .block()

    logInfo(
      s"Completed bulk KV remove op ${opUUID} against ${remove.length} ids in ${NANOSECONDS
          .toMillis(System.nanoTime - start)}ms"
    )

    out
  }

  def mutateIn(
      config: CouchbaseConfig,
      keyspace: Keyspace,
      mutateIn: Seq[MutateIn],
      mutateInOptions: MutateInOptions = null,
      connectionIdentifier: Option[String]
  ): Seq[MutateInResult] = {
    val connection = CouchbaseConnection(connectionIdentifier)
    val cluster    = connection.cluster(config)
    val start      = System.nanoTime()
    val opUUID     = UUID.randomUUID().toString.substring(0, 6)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull)
      .getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (mutateInOptions == null) {
      MutateInOptions()
    } else {
      mutateInOptions
    }

    logInfo(
      s"Performing bulk KV mutateIn against ${mutateIn.length} ids with options $options.  " +
        s"See TRACE logging for all ids"
    )
    logTrace(s"Performing bulk KV mutateIn against ids ${mutateIn.map(_.id)} with options $options")

    val out = SFlux
      .fromIterable(mutateIn)
      .flatMap(doc => collection.mutateIn(doc.id, doc.specs, options.cas(doc.cas)))
      .collectSeq()
      .block()

    logInfo(
      s"Completed bulk KV mutateIn op ${opUUID} against ${mutateIn.length} ids in ${NANOSECONDS
          .toMillis(System.nanoTime - start)}ms"
    )

    out
  }
}
