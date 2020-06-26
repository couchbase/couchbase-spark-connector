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
package com.couchbase

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.document.{JsonArrayDocument, JsonDocument, Document}

package object spark {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
  implicit def toRDDFunctions[T](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions(rdd)
  implicit def toDocumentRDDFunctions[D <: Document[_]](rdd: RDD[D]): DocumentRDDFunctions[D] =
    new DocumentRDDFunctions(rdd)
  implicit def toPairRDDFunctions[V](rdd: RDD[(String, V)]): PairRDDFunctions[V] =
    new PairRDDFunctions(rdd)

  implicit object JsonObjectToJsonDocumentConverter
    extends DocumentConverter[JsonDocument, JsonObject] {
    def documentClassTag(ct: ClassTag[JsonObject]): ClassTag[JsonDocument] =
      implicitly[ClassTag[JsonDocument]]
    override def convert(id: String, content: JsonObject): JsonDocument =
      JsonDocument.create(id, content)
  }

  implicit object JsonArrayToJsonArrayDocumentConverter
    extends DocumentConverter[JsonArrayDocument, JsonArray] {
    def documentClassTag(ct: ClassTag[JsonArray]): ClassTag[JsonArrayDocument] =
      implicitly[ClassTag[JsonArrayDocument]]
    override def convert(id: String, content: JsonArray): JsonArrayDocument =
      JsonArrayDocument.create(id, content)
  }

  implicit object MapToJsonDocumentConverter
    extends DocumentConverter[JsonDocument, Map[String, _]] {
    def documentClassTag(ct: ClassTag[Map[String, _]]): ClassTag[JsonDocument] =
      implicitly[ClassTag[JsonDocument]]
    override def convert(id: String, content: Map[String, _]): JsonDocument = {
      val data = JsonObject.create()
      content.foreach(pair => data.put(pair._1, pair._2))
      JsonDocument.create(id, data)
    }
  }

  implicit object SeqToJsonArrayDocumentConverter
    extends DocumentConverter[JsonArrayDocument, Seq[_]] {
    def documentClassTag(ct: ClassTag[Seq[_]]): ClassTag[JsonArrayDocument] =
      implicitly[ClassTag[JsonArrayDocument]]
    override def convert(id: String, content: Seq[_]): JsonArrayDocument = {
      val data = JsonArray.create()
      content.foreach(item => data.add(item))
      JsonArrayDocument.create(id, data)
    }
  }
}
