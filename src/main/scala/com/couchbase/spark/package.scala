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
package com.couchbase

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{JsonDocument, Document}
import com.couchbase.spark.rdd.DocumentRDDFunctions

package object spark {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions = new SparkContextFunctions(sc)
  implicit def toRDDFunctions[T](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions(rdd)
  implicit def toDocumentRDDFunctions[D <: Document[_]](rdd: RDD[D]): DocumentRDDFunctions[D] = new DocumentRDDFunctions(rdd)
  implicit def toPairRDDFunctions[V](rdd: RDD[(String, V)]): PairRDDFunctions[V] = new PairRDDFunctions(rdd)

  implicit object JsonDocumentConverter extends DocumentConverter[JsonDocument, JsonObject]{

    def documentClassTag(ct: ClassTag[JsonObject]): ClassTag[JsonDocument] = implicitly[ClassTag[JsonDocument]]

    override def convert(id: String, content: JsonObject): JsonDocument = ??? // How to convert?
  }
}