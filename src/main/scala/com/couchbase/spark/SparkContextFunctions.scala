package com.couchbase.spark

import com.couchbase.client.java.document.{RawJsonDocument, Document, JsonDocument}
import com.couchbase.spark.connection.CouchbaseConfig
import com.couchbase.spark.rdd.DocumentRDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def couchbaseGet[D <: Document[_]: ClassTag](id: String): DocumentRDD[D] = {
    couchbaseGet(Seq(id)).asInstanceOf[DocumentRDD[D]]
  }

  def couchbaseGet[D <: Document[_]](ids: Seq[String])(implicit ct: ClassTag[D]) = {
    ct match {
      case ClassTag.Nothing => new DocumentRDD[JsonDocument](sc, new CouchbaseConfig(sc), ids, 1)
      case _ => new DocumentRDD[D](sc, new CouchbaseConfig(sc), ids, 1)
    }
  }

}
