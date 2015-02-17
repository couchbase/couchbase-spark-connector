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
package com.couchbase.spark.rdd

import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{Query, Statement}
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

import rx.lang.scala.JavaConversions._

case class CouchbaseQueryRow(value: JsonObject)

class QueryRDD(@transient sc: SparkContext, bucketName: String = null, query: Query)
  extends RDD[CouchbaseQueryRow](sc, Nil)  {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseQueryRow] = {
    val bucket = CouchbaseConnection().bucket(bucketName, cbConfig).async()

    toScalaObservable(bucket.query(query))
      .flatMap(_.rows())
      .map(row => CouchbaseQueryRow(row.value()))
      .toBlocking
      .toIterable
      .iterator
  }

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))
}

object QueryRDD {

  def apply(sc: SparkContext, bucketName: String, query: Query) = new QueryRDD(sc, bucketName, query)

}