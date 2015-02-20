package com.couchbase.spark.streaming

import com.couchbase.client.java.document.Document
import com.couchbase.spark.connection.{CouchbaseBucket, CouchbaseConnection, CouchbaseConfig}
import org.apache.spark.streaming.dstream.DStream
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable


class DStreamFunctions[D <: Document[_]](dstream: DStream[D]) extends Serializable {

  def sparkContext = dstream.context.sparkContext

  def saveToCouchbase(bucketName: String = null): Unit = {
    val cbConfig = CouchbaseConfig(sparkContext.getConf)

    dstream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        if (iter.nonEmpty) {
          val bucket = CouchbaseConnection().bucket(bucketName, cbConfig).async()
          Observable
            .from(iter.toIterable)
            .flatMap(doc => toScalaObservable(bucket.upsert[D](doc)))
            .toBlocking
            .last
        }
      }
    }

  }



}
