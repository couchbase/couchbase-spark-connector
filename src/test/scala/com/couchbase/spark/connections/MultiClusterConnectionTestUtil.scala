package com.couchbase.spark.connections

import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.spark.kv.LookupIn
import com.couchbase.spark.query.QueryOptions
import com.couchbase.spark.toSparkContextFunctions
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals

object MultiClusterConnectionTestUtil {
  def runStandardSQLQuery(spark: SparkSession, id: String) {
    val result = spark.read
      .format("couchbase.query")
      .option(QueryOptions.ConnectionIdentifier, id)
      .load()
      .limit(3)
      .collect()

    assertEquals(3, result.length)
  }

  def runStandardRDDQuery(spark: SparkSession, id: String) {
    val result = spark.sparkContext
      .couchbaseLookupIn(Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))), connectionIdentifier = id)
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }

  def runStandardRDDQuery(spark: SparkSession) {
    val result = spark.sparkContext
      .couchbaseLookupIn(Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))))
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }
}
