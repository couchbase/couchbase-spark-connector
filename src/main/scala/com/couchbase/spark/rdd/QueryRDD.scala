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

package com.couchbase.spark.rdd

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query._
import com.couchbase.spark.core.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.concurrent.duration.Duration

class QueryRDD (
 @transient private val sc: SparkContext,
 private[spark] val statement: String,
 private[spark] val options: CouchbaseQueryOptions = CouchbaseQueryOptions(),
) extends RDD[CouchbaseQueryResult](sc, Nil) {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseQueryResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(globalConfig)

    val result = cluster.query(statement, options.toQueryOptions).get
    val rows = result.rowsAs[JsonObject].get

    Array(CouchbaseQueryResult(rows, result.metaData)).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(QueryPartition(0))
  }

}

case class QueryPartition(index: Int) extends Partition()

case class CouchbaseQueryOptions(
  parameters: Option[QueryParameters] = None,
  clientContextId: Option[String] = None,
  maxParallelism: Option[Int] = None,
  metrics: Boolean = false,
  pipelineBatch: Option[Int] = None,
  pipelineCap: Option[Int] = None,
  profile: Option[QueryProfile] = None,
  readonly: Option[Boolean] = None,
  retryStrategy: Option[RetryStrategy] = None,
  scanCap: Option[Int] = None,
  scanConsistency: Option[QueryScanConsistency] = None,
  timeout: Option[Duration] = None,
  adhoc: Boolean = true,
  raw: Option[Map[String, Any]] = None,
  flexIndex: Boolean = false
) {

  def toQueryOptions: QueryOptions = {
    var opts = QueryOptions()

    if (scanConsistency.isDefined) {
      opts = opts.scanConsistency(scanConsistency.get)
    }

    if (parameters.isDefined) {
      opts = opts.parameters(parameters.get)
    }

    if (clientContextId.isDefined) {
      opts = opts.clientContextId(clientContextId.get)
    }

    if (maxParallelism.isDefined) {
      opts = opts.maxParallelism(maxParallelism.get)
    }

    if (metrics) {
      opts = opts.metrics(metrics)
    }

    if (pipelineBatch.isDefined) {
      opts = opts.pipelineBatch(pipelineBatch.get)
    }

    if (pipelineCap.isDefined) {
      opts = opts.pipelineCap(pipelineCap.get)
    }

    if (profile.isDefined) {
      opts = opts.profile(profile.get)
    }

    if (readonly.isDefined) {
      opts = opts.readonly(readonly.get)
    }

    if (retryStrategy.isDefined) {
      opts = opts.retryStrategy(retryStrategy.get)
    }

    if (scanCap.isDefined) {
      opts = opts.scanCap(scanCap.get)
    }

    if (timeout.isDefined) {
      opts = opts.timeout(timeout.get)
    }

    if (adhoc) {
      opts = opts.adhoc(adhoc)
    }

    if (flexIndex) {
      opts = opts.flexIndex(flexIndex)
    }

    if (timeout.isDefined) {
      opts = opts.timeout(timeout.get)
    }

    if (raw.isDefined) {
      opts = opts.raw(raw.get)
    }

    opts
  }

}

case class CouchbaseQueryResult(rows: Seq[JsonObject], metaData: QueryMetaData)