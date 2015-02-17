/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark

import com.couchbase.client.java.document.Document
import com.couchbase.client.java.query.{Query, Statement}
import com.couchbase.client.java.view.{SpatialViewQuery, ViewQuery}
import com.couchbase.spark.rdd.{QueryRDD, SpatialViewRDD, ViewRDD}
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def couchbaseGet[D <: Document[_]: ClassTag](bucketName: String = null, ids: Seq[String], numSlices: Int = sc.defaultParallelism): RDD[D] = {
    sc.parallelize(ids, numSlices).couchbaseGet(bucketName)
  }

  def couchbaseView(bucketName: String = null, query: ViewQuery) = ViewRDD(sc, bucketName, query)

  def couchbaseSpatialView(bucketName: String = null, query: SpatialViewQuery) = SpatialViewRDD(sc, bucketName, query)

  def couchbaseQuery(bucketName: String = null, query: Query) = QueryRDD(sc, bucketName, query)
}
