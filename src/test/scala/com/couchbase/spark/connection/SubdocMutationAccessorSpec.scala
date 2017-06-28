package com.couchbase.spark.connection

import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by daschl on 04/07/17.
  */
class SubdocMutationAccessorSpec extends FlatSpec with Matchers {

  "A SubdocMutationAccessor" should "upsert a path into a doc" in {
    val sparkCfg = new SparkConf()
    sparkCfg.set("com.couchbase.username", "Administrator")
    sparkCfg.set("com.couchbase.password", "password")
    val cfg = CouchbaseConfig(sparkCfg)

    val accessor = new SubdocMutationAccessor(cfg, Seq(
      SubdocUpsert("doc", "element", "value"),
      SubdocUpsert("doc2", "_", 5678),
      SubdocUpsert("doc2", "element2", 1234)
    ))

    accessor.compute().foreach(println)
  }

}
