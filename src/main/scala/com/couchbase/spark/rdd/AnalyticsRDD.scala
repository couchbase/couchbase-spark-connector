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
import com.couchbase.client.scala.analytics.{AnalyticsMetaData, AnalyticsOptions, AnalyticsParameters, AnalyticsScanConsistency}
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.core.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.concurrent.duration.Duration

class AnalyticsRDD (
 @transient private val sc: SparkContext,
 private[spark] val statement: String,
 private[spark] val options: CouchbaseAnalyticsOptions = CouchbaseAnalyticsOptions(),
) extends RDD[CouchbaseAnalyticsResult](sc, Nil) {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)

  override def compute(split: Partition,
                       context: TaskContext): Iterator[CouchbaseAnalyticsResult] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(globalConfig)

    val result = cluster.analyticsQuery(statement, options.toAnalyticsOptions).get
    val rows = result.rowsAs[JsonObject].get

    Array(CouchbaseAnalyticsResult(rows, result.metaData)).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(AnalyticsPartition(0))
  }

}

case class AnalyticsPartition(index: Int) extends Partition()

case class CouchbaseAnalyticsOptions(
  parameters: Option[AnalyticsParameters] = None,
  clientContextId: Option[String] = None,
  retryStrategy: Option[RetryStrategy] = None,
  timeout: Option[Duration] = None,
  priority: Boolean = false,
  readonly: Option[Boolean] = None,
  scanConsistency: Option[AnalyticsScanConsistency] = None,
  raw: Option[Map[String, Any]] = None
) {

  def toAnalyticsOptions: AnalyticsOptions = {
    var opts = AnalyticsOptions()

    if (parameters.isDefined) {
      opts = opts.parameters(parameters.get)
    }

    if (clientContextId.isDefined) {
      opts = opts.clientContextId(clientContextId.get)
    }

    if (retryStrategy.isDefined) {
      opts = opts.retryStrategy(retryStrategy.get)
    }

    if (timeout.isDefined) {
      opts = opts.timeout(timeout.get)
    }

    if (readonly.isDefined) {
      opts = opts.readonly(readonly.get)
    }

    if (priority) {
      opts = opts.priority(priority)
    }

    if (scanConsistency.isDefined) {
      opts = opts.scanConsistency(scanConsistency.get)
    }

    if (raw.isDefined) {
      opts = opts.raw(raw.get)
    }

    opts
  }
}

case class CouchbaseAnalyticsResult(rows: Seq[JsonObject], metaData: AnalyticsMetaData)