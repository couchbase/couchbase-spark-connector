package com.couchbase.spark.streaming

import com.couchbase.client.java.document.Document
import com.couchbase.spark._
import org.apache.spark.streaming.dstream.DStream


class DStreamFunctions[D <: Document[_]](dstream: DStream[D]) extends Serializable {

  def sparkContext = dstream.context.sparkContext

  def saveToCouchbase(bucketName: String = null): Unit = {
    dstream.foreachRDD(_.saveToCouchbase(bucketName))
  }



}
