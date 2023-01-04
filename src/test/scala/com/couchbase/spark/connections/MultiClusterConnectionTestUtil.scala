package com.couchbase.spark.connections

import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.client.scala.manager.collection.CollectionSpec
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.kv.LookupIn
import com.couchbase.spark.query.QueryOptions
import com.couchbase.spark.toSparkContextFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.assertEquals
import org.testcontainers.couchbase.CouchbaseContainer

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

  def prepareSampleData(container: CouchbaseContainer, bucketName: String, scopeName: String, collectionName: String): Unit = {
    val initial = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config(s"spark.couchbase.connectionString", container.getConnectionString)
      .config(s"spark.couchbase.username", container.getUsername)
      .config(s"spark.couchbase.password", container.getPassword)
      .config(s"spark.couchbase.implicitBucket", bucketName)
      .getOrCreate()

    val bucket =
      CouchbaseConnection().bucket(CouchbaseConfig(initial.sparkContext.getConf), Some(bucketName))

    bucket.collections.createScope(scopeName)
    bucket.collections.createCollection(CollectionSpec(collectionName, scopeName))

    val airports = initial.read
      .json("src/test/resources/airports.json")
      .withColumn("type", lit("airport"))

    airports.write.format("couchbase.kv").save()

    initial.stop()
  }
}
