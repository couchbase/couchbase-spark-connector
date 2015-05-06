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

import com.couchbase.client.java.document.Document
import com.couchbase.spark.connection.{KeyValueAccessor, CouchbaseConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, Logging, SparkContext}

import scala.reflect.ClassTag


class KeyValueRDD[D <: Document[_]]
  (@transient sc: SparkContext, ids: Seq[String], bucketName: String = null)
  (implicit ct: ClassTag[D])
  extends RDD[D](sc, Nil)
  with Logging {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    new KeyValueAccessor[D](cbConfig, ids, bucketName).compute()
  }

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

}
