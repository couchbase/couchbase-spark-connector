package com.couchbase.spark.rdd

import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark.connection.{CouchbaseConnection, CouchbaseConfig}
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

import rx.lang.scala.JavaConversions._

case class CouchbaseViewRow(id: String, key: Any, value: Any)

class ViewRDD(@transient sc: SparkContext, bucketName: String = null, viewQuery: ViewQuery) extends RDD[CouchbaseViewRow](sc, Nil) {

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseViewRow] = {
    val bucket = CouchbaseConnection().bucket(bucketName, cbConfig).async()

    toScalaObservable(bucket.query(viewQuery))
      .flatMap(result => toScalaObservable(result.rows()))
      .map(row => CouchbaseViewRow(row.id(), row.key(), row.value()))
      .toBlocking
      .toIterable
      .iterator
  }

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

}

object ViewRDD {
  def apply(sc: SparkContext, bucketName: String, viewQuery: ViewQuery) = new ViewRDD(sc, bucketName, viewQuery)
}
