package com.couchbase.spark.rdd

import com.couchbase.client.java.view.{AsyncViewResult, AsyncViewRow, ViewQuery}
import com.couchbase.spark.connection.{CouchbaseConnection, CouchbaseConfig}
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD
import rx.Observable
import rx.functions.Func1

import scala.collection.JavaConversions._

case class CouchbaseViewRow(id: String, key: Any, value: Any)

class ViewRDD(sc: SparkContext,viewQuery: ViewQuery) extends RDD[CouchbaseViewRow](sc, Nil) {

  val cbConfig = CouchbaseConfig(sc.getConf)

  override def compute(split: Partition, context: TaskContext): Iterator[CouchbaseViewRow] = {
    CouchbaseConnection()
      .bucket(cbConfig)
      .async()
      .query(viewQuery)
      .flatMap(new Func1[AsyncViewResult, Observable[AsyncViewRow]] {
        override def call(result: AsyncViewResult) = {
          result.rows()
        }
      })
      .map[CouchbaseViewRow](new Func1[AsyncViewRow, CouchbaseViewRow] {
        override def call(row: AsyncViewRow) = {
          new CouchbaseViewRow(row.id(), row.key(), row.value())
        }
      })
      .toBlocking
      .getIterator
  }

  override protected def getPartitions: Array[Partition] = Array(new CouchbasePartition(0))

}
