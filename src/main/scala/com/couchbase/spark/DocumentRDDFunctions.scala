package com.couchbase.spark

import com.couchbase.client.java.document.Document
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.rdd.RDD
import rx.lang.scala.Observable
import rx.lang.scala.JavaConversions._

class DocumentRDDFunctions[D <: Document[_]](rdd: RDD[D]) extends Serializable {

  private val cbConfig = CouchbaseConfig(rdd.context.getConf)

  def saveToCouchbase(bucketName: String = null): Unit = {
    rdd.foreachPartition(iter => {
      if (iter.nonEmpty) {
        val bucket = CouchbaseConnection().bucket(bucketName, cbConfig).async()
        Observable
          .from(iter.toIterable)
          .flatMap(doc => toScalaObservable(bucket.upsert[D](doc)))
          .toBlocking
          .last
      }
    })
  }

}
