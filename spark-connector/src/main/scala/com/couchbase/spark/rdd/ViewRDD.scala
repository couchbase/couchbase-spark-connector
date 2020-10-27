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

import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.connection.{CouchbaseConfig, ViewAccessor}
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration

case class CouchbaseViewRow(id: String, key: Any, value: Any)

class ViewRDD(@transient private val sc: SparkContext, viewQuery: ViewQuery,
              bucketName: String = null, timeout: Option[Duration] = None, deps: Seq[Dependency[_]])
  extends RDD[CouchbaseViewRow](sc, deps) {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseViewRow] =
    new ViewAccessor(cbConfig, Seq(viewQuery), bucketName, timeout).compute()


  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

}

object ViewRDD {
  def apply(sc: SparkContext, bucketName: String, viewQuery: ViewQuery,
            timeout: Option[Duration] = None) =
    new ViewRDD(sc, viewQuery, bucketName, timeout, Nil)
}
