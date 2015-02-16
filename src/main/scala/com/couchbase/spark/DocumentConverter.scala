package com.couchbase.spark

import scala.reflect.ClassTag

import com.couchbase.client.java.document.Document

trait DocumentConverter[D <: Document[T], T] extends Serializable {

  def documentClassTag(ct: ClassTag[T]): ClassTag[D]

  def convert(id: String, content: T): D
}
