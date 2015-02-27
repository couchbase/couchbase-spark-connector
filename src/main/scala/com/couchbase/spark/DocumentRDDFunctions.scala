package com.couchbase.spark

import com.couchbase.client.java.document.Document
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.internal.OnceIterable
import org.apache.spark.rdd.RDD
import rx.lang.scala.Observable
import rx.lang.scala.JavaConversions._

class DocumentRDDFunctions[D <: Document[_]](rdd: RDD[D]) extends Serializable {

  private val cbConfig = CouchbaseConfig(rdd.context.getConf)

  def saveToCouchbase(bucketName: String = null): Unit = {
    rdd.foreachPartition(iter => {
      if (iter.nonEmpty) {
        val bucket = CouchbaseConnection().bucket(cbConfig, bucketName).async()
        Observable
          .from(OnceIterable(iter))
          .flatMap(doc => toScalaObservable(bucket.upsert[D](doc)))
          .toBlocking
          .last
      }
    })
  }

}
