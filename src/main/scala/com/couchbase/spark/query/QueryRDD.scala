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
package com.couchbase.spark.query

import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import com.couchbase.client.scala.query.{QueryOptions => CouchbaseQueryOptions}

import scala.reflect.ClassTag

class QueryRDD[T: ClassTag](
  @transient private val sc: SparkContext,
  val statement: String,
  val queryOptions: CouchbaseQueryOptions = null,
)(implicit deserializer: JsonDeserializer[T]) extends RDD[T](sc, Nil) with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(globalConfig)

    var options = if (this.queryOptions == null) {
      CouchbaseQueryOptions()
    } else {
      this.queryOptions
    }
    options = options.metrics(true)

    val result = cluster.query(statement, options).get
    if (result.metaData.metrics.isDefined) {
      logDebug(s"Metrics for query $statement: " + result.metaData.metrics.get)
    }
    result.rowsAs[T].get.iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(QueryPartition(0))
  }

  case class QueryPartition(index: Int) extends Partition()
}