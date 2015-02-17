package com.couchbase.spark

import scala.reflect.ClassTag

import com.couchbase.client.java.document.Document

import org.apache.spark.rdd.RDD

class PairRDDFunctions[V](rdd: RDD[(String, V)]) {

  def toCouchbaseDocument[D <: Document[_] : ClassTag](implicit converter: DocumentConverter[D, V]): DocumentRDDFunctions[D] = {
    rdd.map(kv => converter.convert(kv._1, kv._2))
  }

}
