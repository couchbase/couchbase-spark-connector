package com.couchbase.spark

import com.couchbase.client.java.document.Document
import org.apache.spark.streaming.dstream.DStream

package object streaming {

  implicit def toDStreamFunctions[D <: Document[_]](ds: DStream[D]): DStreamFunctions[D] = new DStreamFunctions[D](ds)

}
