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
package com.couchbase.spark

import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.internal.{LazyIterator, OnceIterable}
import com.couchbase.spark.rdd.{CouchbaseViewRow, KeyValueRDD, ViewRDD}
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.reflect.ClassTag
import com.couchbase.client.java.document.Document
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection, KeyValueAccessor}
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD

class RDDFunctions[T](rdd: RDD[T]) extends Serializable {

  private val cbConfig = CouchbaseConfig(rdd.sparkContext.getConf)

  /**
   * Convert a RDD[String] to a RDD[D]. It's available if T is String.
   *
   * @param ct
   * @param evidence
   * @tparam D
   * @return
   */
  def couchbaseGet[D <: Document[_]](bucketName: String = null)
    (implicit ct: ClassTag[D], evidence: RDD[T] <:< RDD[String]): RDD[D] = {

    val idRDD: RDD[String] = rdd
    idRDD.mapPartitions { valueIterator =>
      if (valueIterator.isEmpty) {
        Iterator[D]()
      } else {
        new KeyValueAccessor[D](cbConfig, OnceIterable(valueIterator).toSeq, bucketName).compute()
      }
    }
  }

}
