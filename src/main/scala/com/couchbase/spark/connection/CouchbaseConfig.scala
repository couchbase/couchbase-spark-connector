package com.couchbase.spark.connection

import org.apache.spark.SparkContext

/**
 * .
 *
 * @author Michael Nitschinger
 * @since
 */
class CouchbaseConfig(ctx: SparkContext) extends Serializable {

  val host = ctx.getConf.get("couchbase.host", "127.0.0.1")
  val bucket = ctx.getConf.get("couchbase.bucket", "default")
  val password = ctx.getConf.get("couchbase.password", "")

}
