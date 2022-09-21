package com.couchbase.spark.dsoptions

import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.client.scala.manager.collection.CollectionSpec
import com.couchbase.spark.config._
import com.couchbase.spark.kv.{Get, LookupIn}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}

import java.util
import java.util.UUID

@TestInstance(Lifecycle.PER_CLASS)
class QueryDSOptionsTest {
  var container: CouchbaseContainer = _
  var spark: SparkSession = _
  val connectionProps = new util.HashMap[String,String]()

  private val bucketName = UUID.randomUUID().toString
  private val scopeName = UUID.randomUUID().toString
  private val airportCollectionName = UUID.randomUUID().toString

  @BeforeAll
  def setup(): Unit = {
    container = new CouchbaseContainer("couchbase/server:7.0.3")
      .withBucket(new BucketDefinition(bucketName))
    container.start()

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    connectionProps.put(DSConfigOptions.ConnectionString,container.getConnectionString)
    connectionProps.put(DSConfigOptions.Username,container.getUsername)
    connectionProps.put(DSConfigOptions.Password,container.getPassword)
    connectionProps.put(DSConfigOptions.Bucket,bucketName)
    connectionProps.put(DSConfigOptions.StreamFrom,DSConfigOptions.StreamFromBeginning)
    connectionProps.put(DSConfigOptions.Scope, scopeName)
    connectionProps.put(DSConfigOptions.Collection, airportCollectionName)

    val bucket = CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(connectionProps)).bucket(Some(bucketName))

    bucket.collections.createScope(scopeName)
    bucket.collections.createCollection(CollectionSpec(airportCollectionName, scopeName))

    bucket.scope(scopeName).query(s"create primary index on `$airportCollectionName`")

  }

  @AfterAll
  def teardown(): Unit = {
    CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(connectionProps)).stop()
    container.stop()
    spark.stop()
  }

  @Test
  def queryWriteTest(): Unit = {
    val airports = spark
      .read
      .json("src/test/resources/airports.json")

    airports.write
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,container.getConnectionString)
      .option(DSConfigOptions.Username,container.getUsername)
      .option(DSConfigOptions.Password,container.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.StreamFrom,DSConfigOptions.StreamFromBeginning)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, airportCollectionName)
      .save()

    import com.couchbase.spark._

    val result = spark
      .sparkContext
      .couchbaseLookupIn(Seq(LookupIn("airport::sfo", Seq(LookupInSpec.get("iata")))),connectionOptions=connectionProps)
      .collect()

    assertEquals(1, result.length)
    assertEquals("SFO", result.head.contentAs[String](0).get)
  }

  @Test
  def queryReadTest(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,container.getConnectionString)
      .option(DSConfigOptions.Username,container.getUsername)
      .option(DSConfigOptions.Password,container.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, airportCollectionName)
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }
}
