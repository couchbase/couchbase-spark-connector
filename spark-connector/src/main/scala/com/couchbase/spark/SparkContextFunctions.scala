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

import com.couchbase.client.java.analytics.AnalyticsQuery
import com.couchbase.client.java.document.Document
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.view.{SpatialViewQuery, ViewQuery}
import com.couchbase.spark.connection.{SubdocLookupResult, SubdocLookupSpec, SubdocMutationResult, SubdocMutationSpec}
import com.couchbase.spark.rdd._
import org.apache.spark.SparkContext

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

import scala.concurrent.duration.Duration

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def couchbaseGet[D <: Document[_]: ClassTag](ids: Seq[String], bucketName: String = null,
    numSlices: Int = sc.defaultParallelism, timeout: Option[Duration] = None): RDD[D] = {
    new KeyValueRDD[D](sc, ids, bucketName, timeout)
  }

  def couchbaseSubdocLookup(ids: Seq[String], get: Seq[String], timeout: Option[Duration])
    : RDD[SubdocLookupResult] = couchbaseSubdocLookup(ids, get, Seq(), null, timeout)

  def couchbaseSubdocLookup(ids: Seq[String], get: Seq[String])
  : RDD[SubdocLookupResult] = couchbaseSubdocLookup(ids, get, Seq(), null, None)

  def couchbaseSubdocLookup(ids: Seq[String], get: Seq[String], exists: Seq[String])
  : RDD[SubdocLookupResult] = couchbaseSubdocLookup(ids, get, exists, null, None)

  def couchbaseSubdocLookup(ids: Seq[String], get: Seq[String], exists: Seq[String],
                            timeout: Option[Duration])
  : RDD[SubdocLookupResult] = couchbaseSubdocLookup(ids, get, exists, null, timeout)

  def couchbaseSubdocLookup(ids: Seq[String], get: Seq[String], exists: Seq[String],
    bucketName: String, timeout: Option[Duration] = None): RDD[SubdocLookupResult] = {
    new SubdocLookupRDD(sc, ids.map(SubdocLookupSpec(_, get, exists)), bucketName, timeout)
  }

  def couchbaseSubdocMutate(specs: Seq[SubdocMutationSpec], bucketName: String,
                            timeout: Option[Duration]):
    RDD[SubdocMutationResult] = {
    new SubdocMutateRDD(sc, specs, bucketName, timeout)
  }

  def couchbaseSubdocMutate(specs: Seq[SubdocMutationSpec], bucketName: String):
  RDD[SubdocMutationResult] = {
    new SubdocMutateRDD(sc, specs, bucketName, None)
  }

  def couchbaseSubdocMutate(specs: Seq[SubdocMutationSpec], timeout: Option[Duration] = None):
  RDD[SubdocMutationResult] = {
    couchbaseSubdocMutate(specs, null, timeout)
  }

  def couchbaseView(query: ViewQuery, bucketName: String = null,
                    timeout: Option[Duration] = None) = ViewRDD(sc, bucketName, query, timeout)

  def couchbaseSpatialView(query: SpatialViewQuery,
                           bucketName: String = null, timeout: Option[Duration] = None) =
    SpatialViewRDD(sc, bucketName, query, timeout)

  def couchbaseQuery(query: N1qlQuery, bucketName: String = null,
                     timeout: Option[Duration] = None) = QueryRDD(sc, bucketName, query, timeout)

  def couchbaseAnalytics(query: AnalyticsQuery, bucketName: String = null,
                         timeout: Option[Duration] = None): AnalyticsRDD = {
    new AnalyticsRDD(sc, query, bucketName, timeout)
  }
}
