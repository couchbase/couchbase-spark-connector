package com.couchbase.spark.multiclusterconn

import com.couchbase.client.scala.manager.collection.CollectionSpec
import com.couchbase.spark.config._
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.testcontainers.couchbase.{BucketDefinition, CouchbaseContainer}
import org.apache.spark.sql.functions.{col, lit, split}

import java.util
import java.util.UUID

@TestInstance(Lifecycle.PER_CLASS)
class MultiClusterConnTest {
  var airportContainer: CouchbaseContainer = _
  var flightContainer: CouchbaseContainer = _
  var cmbndContainer: CouchbaseContainer = _
  var spark: SparkSession = _
  val airportConnectionProps = new util.HashMap[String,String]()
  val flightConnectionProps = new util.HashMap[String,String]()
  val cmbndConnectionProps = new util.HashMap[String,String]()

  private val bucketName = UUID.randomUUID().toString
  private val scopeName = UUID.randomUUID().toString
  private val collectionName = UUID.randomUUID().toString

  @BeforeAll
  def setup(): Unit = {
    airportContainer = new CouchbaseContainer("couchbase/server:7.0.3")
      .withBucket(new BucketDefinition(bucketName))
      .withCredentials("airport","airport")
    airportContainer.start()

    flightContainer = new CouchbaseContainer("couchbase/server:7.0.3")
      .withBucket(new BucketDefinition(bucketName))
      .withCredentials("flight","flight")
    flightContainer.start()

    cmbndContainer = new CouchbaseContainer("couchbase/server:7.0.3")
      .withBucket(new BucketDefinition(bucketName))
      .withCredentials("combined","combined")
    cmbndContainer.start()

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    airportConnectionProps.put(DSConfigOptions.ConnectionString,airportContainer.getConnectionString)
    airportConnectionProps.put(DSConfigOptions.Username,airportContainer.getUsername)
    airportConnectionProps.put(DSConfigOptions.Password,airportContainer.getPassword)
    airportConnectionProps.put(DSConfigOptions.Bucket,bucketName)
    airportConnectionProps.put(DSConfigOptions.Scope, scopeName)
    airportConnectionProps.put(DSConfigOptions.Collection, collectionName)

    flightConnectionProps.put(DSConfigOptions.ConnectionString,flightContainer.getConnectionString)
    flightConnectionProps.put(DSConfigOptions.Username,flightContainer.getUsername)
    flightConnectionProps.put(DSConfigOptions.Password,flightContainer.getPassword)
    flightConnectionProps.put(DSConfigOptions.Bucket,bucketName)
    flightConnectionProps.put(DSConfigOptions.StreamFrom,DSConfigOptions.StreamFromBeginning)
    flightConnectionProps.put(DSConfigOptions.Scope, scopeName)
    flightConnectionProps.put(DSConfigOptions.Collection, collectionName)

    cmbndConnectionProps.put(DSConfigOptions.ConnectionString,cmbndContainer.getConnectionString)
    cmbndConnectionProps.put(DSConfigOptions.Username,cmbndContainer.getUsername)
    cmbndConnectionProps.put(DSConfigOptions.Password,cmbndContainer.getPassword)
    cmbndConnectionProps.put(DSConfigOptions.Bucket,bucketName)
    cmbndConnectionProps.put(DSConfigOptions.StreamFrom,DSConfigOptions.StreamFromBeginning)
    cmbndConnectionProps.put(DSConfigOptions.Scope, scopeName)
    cmbndConnectionProps.put(DSConfigOptions.Collection, collectionName)

    val airportBucket = CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(airportConnectionProps)).bucket(Some(bucketName))
    airportBucket.collections.createScope(scopeName)
    airportBucket.collections.createCollection(CollectionSpec(collectionName, scopeName))

    val flightBucket = CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(flightConnectionProps)).bucket(Some(bucketName))
    flightBucket.collections.createScope(scopeName)
    flightBucket.collections.createCollection(CollectionSpec(collectionName, scopeName))

    prepareSampleData()
  }

  @AfterAll
  def teardown(): Unit = {
    CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(airportConnectionProps)).stop()
    CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(flightConnectionProps)).stop()
    airportContainer.stop()
    flightContainer.stop()
    spark.stop()
  }

  def prepareSampleData(): Unit = {
    val airports = spark
      .read
      .json("src/test/resources/airports.json")

    val flights = spark
      .read
      .json("src/test/resources/flights.json")

    airports.write
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,airportContainer.getConnectionString)
      .option(DSConfigOptions.Username,airportContainer.getUsername)
      .option(DSConfigOptions.Password,airportContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .save()

    flights.write
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,flightContainer.getConnectionString)
      .option(DSConfigOptions.Username,flightContainer.getUsername)
      .option(DSConfigOptions.Password,flightContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .save()

    val airportBucket = CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(airportConnectionProps)).bucket(Some(bucketName))
    airportBucket.collections.createScope(scopeName)
    airportBucket.collections.createCollection(CollectionSpec(collectionName, scopeName))
    airportBucket.scope(scopeName).query(s"create primary index on `$collectionName`")

    val flightBucket = CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(flightConnectionProps)).bucket(Some(bucketName))
    flightBucket.collections.createScope(scopeName)
    flightBucket.collections.createCollection(CollectionSpec(collectionName, scopeName))
    flightBucket.scope(scopeName).query(s"create primary index on `$collectionName`")
  }

  @Test
  def cluster1ReadTest(): Unit = {

    val airports = spark.read
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,airportContainer.getConnectionString)
      .option(DSConfigOptions.Username,airportContainer.getUsername)
      .option(DSConfigOptions.Password,airportContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

  @Test
  def cluster2ReadTest(): Unit = {

    val flights = spark.read
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,flightContainer.getConnectionString)
      .option(DSConfigOptions.Username,flightContainer.getUsername)
      .option(DSConfigOptions.Password,flightContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()

    assertEquals(5, flights.count)
    flights.foreach(row => {
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

  @Test
  def joinClustersTest: Unit ={

    val airports = spark.read
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,airportContainer.getConnectionString)
      .option(DSConfigOptions.Username,airportContainer.getUsername)
      .option(DSConfigOptions.Password,airportContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()
      .withColumn("id",split(col("__META_ID"),"::")(1))
      .selectExpr("id","name as airport_name")

    val flights = spark.read
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,flightContainer.getConnectionString)
      .option(DSConfigOptions.Username,flightContainer.getUsername)
      .option(DSConfigOptions.Password,flightContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()
      .withColumn("flight_airport",split(col("airport"),"::")(1))
      .selectExpr("flight_airport","name as flight_name")

    val airportFilghts = airports.join(flights,
      airports("id") === flights("flight_airport"),"inner")

    val fltrFlights = airportFilghts.filter(col("flight_airport") === lit("sfo"))

    assertEquals(2, fltrFlights.count)

  }

  @Test
  def cluster3WriteTest: Unit ={

    val airports = spark.read
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,airportContainer.getConnectionString)
      .option(DSConfigOptions.Username,airportContainer.getUsername)
      .option(DSConfigOptions.Password,airportContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()
      .withColumn("id",split(col("__META_ID"),"::")(1))
      .selectExpr("id","name as airport_name")

    val flights = spark.read
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,flightContainer.getConnectionString)
      .option(DSConfigOptions.Username,flightContainer.getUsername)
      .option(DSConfigOptions.Password,flightContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()
      .withColumn("flight_airport",split(col("airport"),"::")(1))
      .selectExpr("__META_ID as flight_id","flight_airport","name as flight_name")

    val airportFilghts = airports.join(flights,
      airports("id") === flights("flight_airport"),"inner")

    val fltrFlights = airportFilghts.filter(col("flight_airport") === lit("sfo"))

    val cmbndBucket = CouchbaseConnectionPool().getConnection(CouchbaseConfig(spark.sparkContext.getConf,false).loadDSOptions(cmbndConnectionProps)).bucket(Some(bucketName))
    cmbndBucket.collections.createScope(scopeName)
    cmbndBucket.collections.createCollection(CollectionSpec(collectionName, scopeName))
    cmbndBucket.scope(scopeName).query(s"create primary index on `$collectionName`")

    fltrFlights.withColumnRenamed("flight_id","__META_ID").write
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,cmbndContainer.getConnectionString)
      .option(DSConfigOptions.Username,cmbndContainer.getUsername)
      .option(DSConfigOptions.Password,cmbndContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .save()

    cmbndBucket.scope(scopeName).query(s"create primary index on `$collectionName`")

    val clstr3Df = spark.read
      .format("couchbase.query")
      .option(DSConfigOptions.ConnectionString,cmbndContainer.getConnectionString)
      .option(DSConfigOptions.Username,cmbndContainer.getUsername)
      .option(DSConfigOptions.Password,cmbndContainer.getPassword)
      .option(DSConfigOptions.Bucket,bucketName)
      .option(DSConfigOptions.Scope, scopeName)
      .option(DSConfigOptions.Collection, collectionName)
      .option(DSConfigOptions.ScanConsistency, DSConfigOptions.RequestPlusScanConsistency)
      .load()

    assertEquals(2, clstr3Df.count)

  }
}
