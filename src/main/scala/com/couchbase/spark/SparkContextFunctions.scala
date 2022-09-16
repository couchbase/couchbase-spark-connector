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
import com.couchbase.client.core.error.{DocumentExistsException, DocumentNotFoundException}

import java.util
import java.util.{HashMap, Map}
import scala.reflect.ClassTag

/**
 * Brings RDD related functions into the spark context when loaded as an import.
 *
 * @param sc the spark context, made available by spark.
 */
class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  /**
   * Fetches a seq of documents.
   *
   * @param ids the document IDs.
   * @param keyspace the optional keyspace to override the implicit configuration.
   * @param getOptions optional parameters to customize the behavior.
   * @return the RDD result.
   */
  def couchbaseGet(ids: Seq[Get],
    keyspace: Keyspace = Keyspace(),
    getOptions: GetOptions = null,
    connectionOptions: Map[String,String] = new HashMap[String,String]()
  ): RDD[GetResult] =
    new GetRDD(sc, ids, keyspace, getOptions, connectionOptions)

  /**
   * Upserts a seq of documents.
   *
   * @param docs the documents to upsert.
   * @param keyspace the optional keyspace to override the implicit configuration.
   * @param upsertOptions optional parameters to customize the behavior.
   * @param serializer the implicit serializer used to encode the data.
   * @tparam T the type of data that should be stored.
   * @return the RDD result.
   */
  def couchbaseUpsert[T](docs: Seq[Upsert[T]],
                   keyspace: Keyspace = Keyspace(),
                      upsertOptions: UpsertOptions = null,
                         connectionOptions: Map[String,String] = new HashMap[String,String]()
                  )(implicit serializer: JsonSerializer[T]): RDD[MutationResult] =
    new UpsertRDD[T](sc, docs, keyspace, upsertOptions, connectionOptions)

  /**
   * Replaces a seq documents.
   *
   * @param docs the documents to replace.
   * @param keyspace the optional keyspace to override the implicit configuration.
   * @param replaceOptions optional parameters to customize the behavior.
   * @param ignoreIfNotFound set to true if individual [[DocumentNotFoundException]] should be ignored.
   * @param serializer the implicit serializer used to encode the data.
   * @tparam T the type of data that should be stored.
   * @return the RDD result.
   */
  def couchbaseReplace[T](docs: Seq[Replace[T]],
                          keyspace: Keyspace = Keyspace(),
                          replaceOptions: ReplaceOptions = null,
                          ignoreIfNotFound: Boolean = false,
                          connectionOptions: Map[String,String] = new HashMap[String,String]()
                        )(implicit serializer: JsonSerializer[T]): RDD[MutationResult] =
    new ReplaceRDD[T](sc, docs, keyspace, replaceOptions, ignoreIfNotFound, connectionOptions)

  /**
   * Inserts a seq of documents.
   *
   * @param docs the documents that should be inserted.
   * @param keyspace the optional keyspace to override the implicit configuration.
   * @param insertOptions optional parameters to customize the behavior.
   * @param ignoreIfExists set to true if individual [[DocumentExistsException]] should be ignored.
   * @param serializer the implicit serializer used to encode the data.
   * @tparam T the type of data that should be stored.
   * @return the RDD result.
   */
  def couchbaseInsert[T](docs: Seq[Insert[T]],
                         keyspace: Keyspace = Keyspace(),
                         insertOptions: InsertOptions = null,
                         ignoreIfExists: Boolean = false,
                         connectionOptions: Map[String,String] = new HashMap[String,String]()
                        )(implicit serializer: JsonSerializer[T]): RDD[MutationResult] =
    new InsertRDD[T](sc, docs, keyspace, insertOptions, ignoreIfExists, connectionOptions)

  /**
   * Removes a seq of documents.
   *
   * @param docs the documents which should be removed.
   * @param keyspace the optional keyspace to override the implicit configuration.
   * @param removeOptions optional parameters to customize the behavior.
   * @param ignoreIfNotFound set to true if individual [[DocumentNotFoundException]] should be ignored.
   * @return the RDD result.
   */
  def couchbaseRemove(docs: Seq[Remove],
                      keyspace: Keyspace = Keyspace(),
                      removeOptions: RemoveOptions = null,
                      ignoreIfNotFound: Boolean = false,
                      connectionOptions: Map[String,String] = new HashMap[String,String]()): RDD[MutationResult] =
    new RemoveRDD(sc, docs, keyspace, removeOptions, ignoreIfNotFound, connectionOptions)

  /**
   * Performs subdocument lookup.
   *
   * @param docs the documents to look up.
   * @param keyspace the optional keyspace to override the implicit configuration.
   * @param lookupInOptions optional parameters to customize the behavior.
   * @return the RDD result.
   */
  def couchbaseLookupIn(docs: Seq[LookupIn],
    keyspace: Keyspace = Keyspace(),
    lookupInOptions: LookupInOptions = null,
    connectionOptions: Map[String,String] = new HashMap[String,String]()
  ): RDD[LookupInResult] =
    new LookupInRDD(sc, docs, keyspace, lookupInOptions, connectionOptions)

  /**
   * Performs subdocument mutations.
   *
   * @param docs the documents to mutate.
   * @param keyspace the optional keyspace to override the implicit configuration.
   * @param mutateInOptions optional parameters to customize the behavior.
   * @return the RDD result.
   */
  def couchbaseMutateIn(docs: Seq[MutateIn],
    keyspace: Keyspace = Keyspace(),
    mutateInOptions: MutateInOptions = null,
    connectionOptions: Map[String,String] = new HashMap[String,String]()
  ): RDD[MutateInResult] =
    new MutateInRDD(sc, docs, keyspace, mutateInOptions, connectionOptions)

  /**
   * Performs a N1QL query.
   *
   * @param statement the query statement to execute.
   * @param queryOptions optional parameters to customize the behavior.
   * @param keyspace the keyspace (only provide bucket and scope if needed).
   * @param deserializer the implicit JSON deserializer to use.
   * @tparam T the document type to decode into.
   * @return the RDD result.
   */
  def couchbaseQuery[T: ClassTag](statement: String, queryOptions: QueryOptions = null, keyspace: Keyspace = null,
                                  connectionOptions: Map[String,String] = new HashMap[String,String]())
                                 (implicit deserializer: JsonDeserializer[T]): RDD[T] =
    new QueryRDD[T](sc, statement, queryOptions, keyspace, connectionOptions)

  /**
   * Performs an analytics query.
   *
   * @param statement the analytics statement to execute.
   * @param analyticsOptions optional parameters to customize the behavior.
   * @param keyspace the keyspace (only provide bucket and scope if needed).
   * @param deserializer the implicit JSON deserializer to use.
   * @tparam T the document type to decode into.
   * @return the RDD result.
   */
  def couchbaseAnalyticsQuery[T: ClassTag](statement: String, analyticsOptions: AnalyticsOptions = null,
                                           keyspace: Keyspace = null,
                                           connectionOptions: Map[String,String] = new HashMap[String,String]())
                                          (implicit deserializer: JsonDeserializer[T]): RDD[T] =
    new AnalyticsRDD[T](sc, statement, analyticsOptions, keyspace, connectionOptions)

  /**
   * Performs a full-text search query.
   *
   * @param indexName the name of the search index.
   * @param query the search query to be sent to the index.
   * @param searchOptions optional parameters to customize the behavior.
   * @return the RDD result.
   */
  def couchbaseSearchQuery(indexName: String, query: SearchQuery, searchOptions: SearchOptions = null,
                           connectionOptions: Map[String,String] = new HashMap[String,String]()): RDD[SearchResult] = {
    new SearchRDD(sc, indexName, query, searchOptions, connectionOptions)
  }

}
