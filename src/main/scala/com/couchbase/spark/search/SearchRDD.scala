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

import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection, CouchbaseConnectionPool}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import java.util.{HashMap, Map}
import collection.JavaConverters._

import com.couchbase.spark.config.mapToSparkConf

class SearchPartition(id: Int, loc: Seq[String]) extends Partition {
  override def index: Int = id
  def location: Seq[String] = loc
  override def toString = s"SearchPartition($id, $loc)"
}

class SearchRDD(@transient private val sc: SparkContext, val indexName: String, val query: SearchQuery,
                val searchOptions: SearchOptions = null,val connectionOptions: Map[String,String] = new HashMap[String,String]())
  extends RDD[SearchResult](sc, Nil)
  with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf,false).loadDSOptions(connectionOptions)

  override def compute(split: Partition, context: TaskContext): Iterator[SearchResult] = {
    val connection = CouchbaseConnectionPool().getConnection(globalConfig)
    val cluster = connection.cluster()

    val options = if(this.searchOptions == null) {
      SearchOptions()
    } else {
      this.searchOptions
    }

    Array(cluster.searchQuery(indexName, query, options).get).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val core = CouchbaseConnectionPool().getConnection(globalConfig).cluster().async.core
    val config = core.clusterConfig()

    val partitions = if (config.globalConfig() != null) {
      Array(new SearchPartition(0, config
        .globalConfig()
        .portInfos()
        .asScala
        .filter(p => p.ports().containsKey(ServiceType.SEARCH))
        .map(p => {
          val aa = core.context().alternateAddress()
          if (aa != null && aa.isPresent) {
            p.alternateAddresses().get(aa.get()).hostname()
          } else {
            p.hostname()
          }
        })))
    } else {
      Array(new SearchPartition(0, Seq())
      )
    }

    logDebug(s"Calculated SearchPartitions operation ${partitions.mkString("Array(", ", ", ")")}")

    partitions.asInstanceOf[Array[Partition]]
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[SearchPartition].location
  }
}
