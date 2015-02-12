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

import com.couchbase.client.java.document.{JsonDocument, Document}
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD
import rx.Observable
import rx.functions.Func1

import scala.collection.JavaConversions._

import scala.reflect.ClassTag

class DocumentRDD[D <: Document[_]]
    (sc: SparkContext, ids: Seq[String], numPartitions: Int)
    (implicit ct: ClassTag[D])
  extends RDD[D](sc, Nil) {

  val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
   Observable
      .from(ids.toArray)
      .flatMap(new Func1[String, Observable[D]] {
        override def call(id: String) = {
          CouchbaseConnection().bucket(cbConfig).async().get(id, ct.runtimeClass.asInstanceOf[Class[D]])
        }
      })
    .toBlocking
    .getIterator
  }

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map(i => new CouchbasePartition(i)).toArray
  }

}
