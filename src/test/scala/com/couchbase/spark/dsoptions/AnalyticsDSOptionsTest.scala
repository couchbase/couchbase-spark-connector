package com.couchbase.spark.dsoptions

import com.couchbase.spark.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer, CouchbaseService}

import java.util
import java.util.UUID

@TestInstance(Lifecycle.PER_CLASS)
class AnalyticsDSOptionsTest {
  var container: CouchbaseContainer = _
  var spark: SparkSession = _
  val connectionProps = new util.HashMap[String,String]()
  val bucketName: String = UUID.randomUUID().toString

  @BeforeAll
  def setup(): Unit = {

    container = new CouchbaseContainer("couchbase/server:6.6.2")
      .withEnabledServices(CouchbaseService.KV, CouchbaseService.ANALYTICS)
      .withBucket(new BucketDefinition(bucketName).withPrimaryIndex(false))
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

    val cluster = CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(connectionProps)).cluster()
    cluster.analyticsIndexes.createDataset("airports", bucketName)
    cluster.analyticsQuery("connect link Local").get

    prepareSampleData()
  }

  @AfterAll
  def teardown(): Unit = {
    CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(connectionProps)).stop()
    container.stop()
    spark.stop()
  }

  private def prepareSampleData(): Unit = {
    val airports = spark
      .read
      .json("src/test/resources/airports.json")
      .withColumn("type", lit("airport"))

    airports.write.format("couchbase.kv")
      .option(DSConfigOptions.ConnectionString,container.getConnectionString)
      .option(DSConfigOptions.Username,container.getUsername)
      .option(DSConfigOptions.Password,container.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.StreamFrom,DSConfigOptions.StreamFromBeginning)
      .save()
  }

  @Test
  def analyticsReadTest(): Unit = {
    val airports = spark.read
      .format("couchbase.analytics")
      .option(DSConfigOptions.ConnectionString,container.getConnectionString)
      .option(DSConfigOptions.Username,container.getUsername)
      .option(DSConfigOptions.Password,container.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Dataset, "airports")
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }
}
