package com.couchbase.spark.rdd

import com.couchbase.client.java.document.{JsonDocument, Document}
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD
import rx.Observable
import rx.functions.Func1

import scala.collection.JavaConversions._

import scala.reflect.ClassTag

class CouchbasePartition(idx: Int) extends Partition {
  override def index = idx
}

class DocumentRDD[D <: Document[_]]
    (sc: SparkContext, cfg: CouchbaseConfig, ids: Seq[String], numPartitions: Int)
    (implicit ct: ClassTag[D])
  extends RDD[D](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    Observable
      .from(ids.toArray)
      .flatMap(new Func1[String, Observable[D]] {
        override def call(id: String) = {
          CouchbaseConnection.get.bucket(cfg).async().get(id, ct.runtimeClass.asInstanceOf[Class[D]])
        }
      })
    .toBlocking
    .getIterator
  }

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map(i => new CouchbasePartition(i)).toArray
  }

}
