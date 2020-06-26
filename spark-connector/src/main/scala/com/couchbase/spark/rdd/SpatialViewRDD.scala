/*
 * Copyright (c) 2015 Couchbase, Inc.
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

import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.view.SpatialViewQuery
import com.couchbase.spark.connection.{CouchbaseConfig, SpatialViewAccessor}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration

case class CouchbaseSpatialViewRow(id: String, key: Any, value: Any, geometry: JsonObject)

class SpatialViewRDD
  (@transient private val sc: SparkContext, viewQuery: SpatialViewQuery, bucketName: String = null,
   timeout: Option[Duration] = None)
  extends RDD[CouchbaseSpatialViewRow](sc, Nil) {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext):
    Iterator[CouchbaseSpatialViewRow] =
    new SpatialViewAccessor(cbConfig, Seq(viewQuery), bucketName, timeout).compute()

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

}

object SpatialViewRDD {
  def apply(sc: SparkContext, bucketName: String, viewQuery: SpatialViewQuery,
            timeout: Option[Duration] = None) =
    new SpatialViewRDD(sc, viewQuery, bucketName, timeout)
}
