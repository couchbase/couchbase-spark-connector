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
package com.couchbase.spark

import com.couchbase.client.scala.analytics.AnalyticsOptions
import com.couchbase.client.scala.codec.{JsonDeserializer, JsonSerializer}
import com.couchbase.client.scala.kv.{GetOptions, GetResult, InsertOptions, LookupInOptions, LookupInResult, MutateInOptions, MutateInResult, MutationResult, RemoveOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.client.scala.query.QueryOptions
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.spark.analytics.AnalyticsRDD
import com.couchbase.spark.kv.{Get, GetRDD, Insert, InsertRDD, LookupIn, LookupInRDD, MutateIn, MutateInRDD, Remove, RemoveRDD, Replace, ReplaceRDD, Upsert, UpsertRDD}
import com.couchbase.spark.query.QueryRDD
import com.couchbase.spark.search.SearchRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def couchbaseGet(ids: Seq[Get],
    keyspace: Keyspace = Keyspace(),
    getOptions: GetOptions = null
  ): RDD[GetResult] =
    new GetRDD(sc, ids, keyspace, getOptions)

  def couchbaseUpsert[T](docs: Seq[Upsert[T]],
                   keyspace: Keyspace = Keyspace(),
                      upsertOptions: UpsertOptions = null
                  )(implicit serializer: JsonSerializer[T]): RDD[MutationResult] =
    new UpsertRDD[T](sc, docs, keyspace, upsertOptions)

  def couchbaseReplace[T](docs: Seq[Replace[T]],
                         keyspace: Keyspace = Keyspace(),
                          replaceOptions: ReplaceOptions = null
                        )(implicit serializer: JsonSerializer[T]): RDD[MutationResult] =
    new ReplaceRDD[T](sc, docs, keyspace, replaceOptions)

  def couchbaseInsert[T](docs: Seq[Insert[T]],
                         keyspace: Keyspace = Keyspace(),
                         insertOptions: InsertOptions = null
                        )(implicit serializer: JsonSerializer[T]): RDD[MutationResult] =
    new InsertRDD[T](sc, docs, keyspace, insertOptions)

  def couchbaseRemove(docs: Seq[Remove],
                         keyspace: Keyspace = Keyspace(),
                      removeOptions: RemoveOptions = null
                        ): RDD[MutationResult] =
    new RemoveRDD(sc, docs, keyspace, removeOptions)

  def couchbaseLookupIn(docs: Seq[LookupIn],
    keyspace: Keyspace = Keyspace(),
    lookupInOptions: LookupInOptions = null
  ): RDD[LookupInResult] =
    new LookupInRDD(sc, docs, keyspace, lookupInOptions)

  def couchbaseMutateIn(docs: Seq[MutateIn],
    keyspace: Keyspace = Keyspace(),
    mutateInOptions: MutateInOptions = null
  ): RDD[MutateInResult] =
    new MutateInRDD(sc, docs, keyspace, mutateInOptions)

  def couchbaseQuery[T: ClassTag](statement: String, queryOptions: QueryOptions = null)
                                 (implicit deserializer: JsonDeserializer[T]): RDD[T] =
    new QueryRDD[T](sc, statement, queryOptions)

  def couchbaseAnalyticsQuery[T: ClassTag](statement: String, analyticsOptions: AnalyticsOptions = null)
                                          (implicit deserializer: JsonDeserializer[T]): RDD[T] =
    new AnalyticsRDD[T](sc, statement, analyticsOptions)

  def couchbaseSearchQuery(indexName: String, query: SearchQuery, searchOptions: SearchOptions = null): RDD[SearchResult] = {
    new SearchRDD(sc, indexName, query, searchOptions)
  }

}
