package com.couchbase.spark.connection

import org.scalatest._

class CouchbaseConnectionSpec extends FlatSpec with Matchers {

  "A Connection" should "not be initialized more than once" in {
    val conn1 = CouchbaseConnection()
    val conn2 = CouchbaseConnection()

    conn1 should equal (conn2)
  }

  "A Connection" should "maintain bucket references" in {
    val conn = CouchbaseConnection()
    val cfg = CouchbaseConfig()

    val bucket1 = conn.bucket(cfg)
    val bucket2 = conn.bucket(cfg)

    bucket1 should equal (bucket2)
  }

}
