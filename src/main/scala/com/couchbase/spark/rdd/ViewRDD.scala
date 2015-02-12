package com.couchbase.spark.rdd

import com.couchbase.client.java.view.{AsyncViewResult, AsyncViewRow, ViewQuery}
import com.couchbase.spark.connection.{CouchbaseConnection, CouchbaseConfig}
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD
import rx.Observable
import rx.functions.Func1
import rx.lang.scala.Observable

import scala.collection.JavaConversions._
import rx.lang.scala.JavaConversions._

case class CouchbaseViewRow(id: String, key: Any, value: Any)

class ViewRDD(sc: SparkContext,viewQuery: ViewQuery) extends RDD[CouchbaseViewRow](sc, Nil) {

  val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseViewRow] = {
    val bucket = CouchbaseConnection().bucket(cbConfig).async()

    toScalaObservable(bucket.query(viewQuery))
      .flatMap(result => toScalaObservable(result.rows()))
      .map(row => CouchbaseViewRow(row.id(), row.key(), row.value()))
      .toBlocking
      .toIterable
      .iterator
  }

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

}
