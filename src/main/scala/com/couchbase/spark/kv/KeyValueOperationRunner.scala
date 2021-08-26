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

import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.kv.{InsertOptions, MutateInOptions, MutateInResult, MutationResult, RemoveOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.spark.{DefaultConstants, Keyspace}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import reactor.core.scala.publisher.SFlux

object KeyValueOperationRunner extends Logging {

  def upsert[T](config: CouchbaseConfig, keyspace: Keyspace, upsert: Seq[Upsert[T]],
                upsertOptions: UpsertOptions = null)(implicit serializer: JsonSerializer[T]): Seq[MutationResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(config)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull).
      getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (upsertOptions == null) {
      UpsertOptions()
    } else {
      upsertOptions
    }

    logDebug(s"Performing bulk upsert against ids ${upsert.map(_.id)} with options $options")

    SFlux
      .fromIterable(upsert)
      .flatMap(doc => collection.upsert(doc.id, doc.content, options))
      .collectSeq()
      .block()
  }

  def insert[T](config: CouchbaseConfig, keyspace: Keyspace, insert: Seq[Insert[T]],
                insertOptions: InsertOptions = null)(implicit serializer: JsonSerializer[T]): Seq[MutationResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(config)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull).
      getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (insertOptions == null) {
      InsertOptions()
    } else {
      insertOptions
    }

    logDebug(s"Performing bulk insert against ids ${insert.map(_.id)} with options $options")

    SFlux
      .fromIterable(insert)
      .flatMap(doc => collection.insert(doc.id, doc.content, options))
      .collectSeq()
      .block()
  }

  def replace[T](config: CouchbaseConfig, keyspace: Keyspace, replace: Seq[Replace[T]],
                 replaceOptions: ReplaceOptions = null)(implicit serializer: JsonSerializer[T]): Seq[MutationResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(config)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull).
      getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (replaceOptions == null) {
      ReplaceOptions()
    } else {
      replaceOptions
    }

    logDebug(s"Performing bulk replace against ids ${replace.map(_.id)} with options $options")

    SFlux
      .fromIterable(replace)
      .flatMap(doc => collection.replace(doc.id, doc.content, options.cas(doc.cas)))
      .collectSeq()
      .block()
  }

  def remove(config: CouchbaseConfig, keyspace: Keyspace, remove: Seq[Remove],
             removeOptions: RemoveOptions = null): Seq[MutationResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(config)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull).
      getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (removeOptions == null) {
      RemoveOptions()
    } else {
      removeOptions
    }

    logDebug(s"Performing bulk remove against ids ${remove.map(_.id)} with options $options")

    SFlux
      .fromIterable(remove)
      .flatMap(doc => collection.remove(doc.id, options.cas(doc.cas)))
      .collectSeq()
      .block()
  }

  def mutateIn(config: CouchbaseConfig, keyspace: Keyspace, mutateIn: Seq[MutateIn],
               mutateInOptions: MutateInOptions = null): Seq[MutateInResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(config)

    val bucketName = config
      .implicitBucketNameOr(keyspace.bucket.orNull)
    val scopeName = config
      .implicitScopeNameOr(keyspace.scope.orNull).
      getOrElse(DefaultConstants.DefaultScopeName)
    val collectionName = config
      .implicitCollectionName(keyspace.collection.orNull)
      .getOrElse(DefaultConstants.DefaultCollectionName)

    val collection = cluster.bucket(bucketName).scope(scopeName).collection(collectionName).reactive
    val options = if (mutateInOptions == null) {
      MutateInOptions()
    } else {
      mutateInOptions
    }

    logDebug(s"Performing bulk MutateIn against ids ${mutateIn.map(_.id)} with options $options")

    SFlux
      .fromIterable(mutateIn)
      .flatMap(doc => collection.mutateIn(doc.id, doc.specs, options.cas(doc.cas)))
      .collectSeq()
      .block()
  }
}
