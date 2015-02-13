package com.couchbase.spark.rdd

import com.couchbase.client.java.document.Document
import com.couchbase.client.java.view.{AsyncViewResult, AsyncViewRow, ViewQuery}
import com.couchbase.spark.connection.{CouchbaseConnection, CouchbaseConfig}
import com.couchbase.spark._
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

import rx.lang.scala.JavaConversions._

import scala.reflect.ClassTag

case class CouchbaseViewRow(id: String, key: Any, value: Any)

class ViewRDD(@transient sc: SparkContext, design: String, view: String) extends RDD[CouchbaseViewRow](sc, Nil) {
  // Use design and view because ViewQuery is not Serializable

  private val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseViewRow] = {
    val viewQuery = ViewQuery.from(design, view)
    val bucket = CouchbaseConnection().bucket(cbConfig).async()

    toScalaObservable(bucket.query(viewQuery))
      .flatMap(result => toScalaObservable(result.rows()))
      .map(row => CouchbaseViewRow(row.id(), row.key(), row.value()))
      .toBlocking
      .toIterable
      .iterator
  }

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

  def documents[D <: Document[_]]()(implicit ct: ClassTag[D]): RDD[D] = map(_.id).filter(_ != null).documents

}
