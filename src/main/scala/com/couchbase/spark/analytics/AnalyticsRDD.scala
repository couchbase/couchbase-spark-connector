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
package com.couchbase.spark.analytics

import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.analytics.{AnalyticsOptions => CouchbaseAnalyticsOptions}
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class AnalyticsRDD[T: ClassTag](
  @transient private val sc: SparkContext,
  val statement: String,
  val analyticsOptions: CouchbaseAnalyticsOptions = null,
)(implicit deserializer: JsonDeserializer[T]) extends RDD[T](sc, Nil) with Logging {

  private val globalConfig = CouchbaseConfig(sparkContext.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val connection = CouchbaseConnection()
    val cluster = connection.cluster(globalConfig)

    val options = if (this.analyticsOptions == null) {
      CouchbaseAnalyticsOptions()
    } else {
      this.analyticsOptions
    }
    val result = cluster.analyticsQuery(statement, options).get
    logDebug(s"Metrics for analytics query $statement: " + result.metaData.metrics)
    result.rowsAs[T].get.iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array(AnalyticsPartition(0))
  }

  case class AnalyticsPartition(index: Int) extends Partition()
}