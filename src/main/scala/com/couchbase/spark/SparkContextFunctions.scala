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

import com.couchbase.spark.rdd.{AnalyticsRDD, CouchbaseAnalyticsOptions, CouchbaseAnalyticsResult, CouchbaseGetOptions, CouchbaseGetRDD, CouchbaseGetResult, CouchbaseQueryOptions, CouchbaseQueryResult, QueryRDD}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def couchbaseGet(documentIds: Seq[String],
                   options: CouchbaseGetOptions = CouchbaseGetOptions())
  : RDD[CouchbaseGetResult] = {
    new CouchbaseGetRDD(sc, documentIds, options)
  }

  def couchbaseQuery(statement: String,
                     options: CouchbaseQueryOptions = CouchbaseQueryOptions())
  : RDD[CouchbaseQueryResult] = {
    new QueryRDD(sc, statement, options)
  }

  def couchbaseAnalyticsQuery(statement: String,
                              options: CouchbaseAnalyticsOptions = CouchbaseAnalyticsOptions())
  : RDD[CouchbaseAnalyticsResult] = {
    new AnalyticsRDD(sc, statement, options)
  }

}
