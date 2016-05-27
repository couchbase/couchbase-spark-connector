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

import com.couchbase.client.java.view.{SpatialViewQuery, ViewQuery}
import com.couchbase.spark.internal.{OnceIterable}
import com.couchbase.spark.rdd._

import scala.reflect.ClassTag
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark.connection._
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

  def couchbaseView(bucketName: String = null)
    (implicit evidence: RDD[T] <:< RDD[ViewQuery]) : RDD[CouchbaseViewRow] = {
    val viewRDD: RDD[ViewQuery] = rdd
    viewRDD.mapPartitions { valueIterator =>
      if (valueIterator.isEmpty) {
        Iterator[CouchbaseViewRow]()
      } else {
        new ViewAccessor(cbConfig, OnceIterable(valueIterator).toSeq, bucketName).compute()
      }
    }
  }

  def couchbaseSpatialView(bucketName: String = null)
    (implicit evidence: RDD[T] <:< RDD[SpatialViewQuery])
    : RDD[CouchbaseSpatialViewRow] = {
    val viewRDD: RDD[SpatialViewQuery] = rdd
    viewRDD.mapPartitions { valueIterator =>
      if (valueIterator.isEmpty) {
        Iterator[CouchbaseSpatialViewRow]()
      } else {
        new SpatialViewAccessor(cbConfig, OnceIterable(valueIterator).toSeq, bucketName).compute()
      }
    }
  }

  def couchbaseQuery(bucketName: String = null)
    (implicit evidence: RDD[T] <:< RDD[N1qlQuery])
  : RDD[CouchbaseQueryRow] = {
    val queryRDD: RDD[N1qlQuery] = rdd
    queryRDD.mapPartitions { valueIterator =>
      if (valueIterator.isEmpty) {
        Iterator[CouchbaseQueryRow]()
      } else {
        new QueryAccessor(cbConfig, OnceIterable(valueIterator).toSeq, bucketName).compute()
      }
    }
  }

  def couchbaseSubdocLookup(get: Seq[String])
    (implicit evidence: RDD[T] <:< RDD[String]): RDD[SubdocLookupResult] =
    couchbaseSubdocLookup(get, Seq(), null)

  def couchbaseSubdocLookup(get: Seq[String], exists: Seq[String])
   (implicit evidence: RDD[T] <:< RDD[String]): RDD[SubdocLookupResult] =
    couchbaseSubdocLookup(get, exists, null)

  def couchbaseSubdocLookup(get: Seq[String], exists: Seq[String], bucketName: String)
    (implicit evidence: RDD[T] <:< RDD[String]): RDD[SubdocLookupResult] = {
    val subdocRDD: RDD[String] = rdd
    subdocRDD.mapPartitions { valueIterator =>
      if (valueIterator.isEmpty) {
        Iterator[SubdocLookupResult]()
      } else {
        val specs = OnceIterable(valueIterator).toSeq.map(SubdocLookupSpec(_, get, exists))
        new SubdocLookupAccessor(cbConfig, specs, bucketName).compute()
      }
    }
  }
}
