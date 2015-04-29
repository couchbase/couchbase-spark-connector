package com.couchbase.spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

class SparkStreamingFunctions(@transient val ssc: StreamingContext) extends Serializable  {

  def couchbaseStream(bucket: String = null): ReceiverInputDStream[StreamMessage] = {
    new CouchbaseInputDStream(ssc, bucket)
  }

}
