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
package com.couchbase.spark.search

import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD


class SearchRDD(@transient private val sc: SparkContext, val indexName: String, val query: SearchQuery,
                val searchOptions: SearchOptions = null)
  extends RDD[SearchResult](sc, Nil)
  with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[SearchResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(globalConfig)

    val options = if(this.searchOptions == null) {
      SearchOptions()
    } else {
      this.searchOptions
    }

    Array(cluster.searchQuery(indexName, query, options).get).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(SearchPartition(0))
  }

  case class SearchPartition(index: Int) extends Partition()
}
