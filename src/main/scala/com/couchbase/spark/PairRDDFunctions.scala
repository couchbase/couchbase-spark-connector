package com.couchbase.spark

import scala.reflect.ClassTag

import com.couchbase.client.java.document.Document
import com.couchbase.spark.rdd.DocumentRDDFunctions

import org.apache.spark.rdd.RDD

// better name??
class PairRDDFunctions[V](rdd: RDD[(String, V)]) {

  def documentRDD[D <: Document[V] : ClassTag](implicit converter: DocumentConverter[D, V]): DocumentRDDFunctions[D] = {
    rdd.map(kv => converter.convert(kv._1, kv._2))
  }

}
