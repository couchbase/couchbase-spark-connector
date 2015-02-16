package com.couchbase.spark.rdd

import com.couchbase.client.java.document.Document
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}

import org.apache.spark.rdd.RDD

class DocumentRDDFunctions[D <: Document[_]](rdd: RDD[D]) extends Serializable {

  private val cbConfig = CouchbaseConfig(rdd.context.getConf)

  def saveToCouchbase(): Unit = {
    rdd.foreachPartition(iter => {
      val bucket = CouchbaseConnection().bucket(cbConfig)
      iter.foreach(bucket.upsert[D])
    })
  }
}
