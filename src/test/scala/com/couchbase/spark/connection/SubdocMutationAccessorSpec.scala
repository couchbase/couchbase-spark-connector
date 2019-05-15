package com.couchbase.spark.connection

import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{Document, JsonDocument}
import org.apache.spark.SparkConf
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by daschl on 04/07/17.
  */
@RunWith(classOf[JUnitRunner])
class SubdocMutationAccessorSpec extends FlatSpec with Matchers {

  "A SubdocMutationAccessor" should "upsert a path into a doc" in {
    val sparkCfg = new SparkConf()
    sparkCfg.set("com.couchbase.username", "Administrator")
    sparkCfg.set("com.couchbase.password", "password")
    val cfg = CouchbaseConfig(sparkCfg)

    val bucket = CouchbaseConnection().bucket(cfg, "default")

    bucket.upsert(JsonDocument.create("doc", JsonObject.create()))
    bucket.upsert(JsonDocument.create("doc2", JsonObject.create()))

    val accessor = new SubdocMutationAccessor(cfg, Seq(
      SubdocUpsert("doc", "element", "value"),
      SubdocUpsert("doc2", "_", 5678),
      SubdocUpsert("doc2", "element2", 1234)
    ), null, None)

    accessor.compute().foreach(println)
  }

}
