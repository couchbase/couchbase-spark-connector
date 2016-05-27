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

import com.couchbase.client.java.document.Document
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.view.{SpatialViewQuery, ViewQuery}
import com.couchbase.spark.connection.{SubdocLookupResult, SubdocLookupSpec}
import com.couchbase.spark.rdd._
import org.apache.spark.SparkContext

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def couchbaseGet[D <: Document[_]: ClassTag](ids: Seq[String], bucketName: String = null,
    numSlices: Int = sc.defaultParallelism): RDD[D] = {
    new KeyValueRDD[D](sc, ids, bucketName)
  }

  def couchbaseSubdocLookup(ids: Seq[String], get: Seq[String])
    : RDD[SubdocLookupResult] = couchbaseSubdocLookup(ids, get, Seq(), null)

  def couchbaseSubdocLookup(ids: Seq[String], get: Seq[String], exists: Seq[String])
  : RDD[SubdocLookupResult] = couchbaseSubdocLookup(ids, get, exists, null)

  def couchbaseSubdocLookup(ids: Seq[String], get: Seq[String], exists: Seq[String],
    bucketName: String): RDD[SubdocLookupResult] = {
    new SubdocLookupRDD(sc, ids.map(SubdocLookupSpec(_, get, exists)), bucketName)
  }

  def couchbaseView(query: ViewQuery, bucketName: String = null) = ViewRDD(sc, bucketName, query)

  def couchbaseSpatialView(query: SpatialViewQuery, bucketName: String = null) =
    SpatialViewRDD(sc, bucketName, query)

  def couchbaseQuery(query: N1qlQuery, bucketName: String = null) = QueryRDD(sc, bucketName, query)
}
