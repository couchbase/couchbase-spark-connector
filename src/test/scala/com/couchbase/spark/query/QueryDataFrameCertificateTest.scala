/*
 * Copyright (c) 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.spark.query

import com.couchbase.spark.config.CouchbaseConfig
import com.couchbase.spark.util.{Params, SparkOperationalTest, TestInfraBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{BeforeAll, Disabled, Test, TestInstance}

/**
 * Tests for certificate authentication.
 * Follow the instructions here to setup the client certificate and the java keystore
 * https://docs.couchbase.com/server/current/manage/manage-security/configure-client-certificates.html
 *
 * Note that the name used in the connectionString must match the name in the node certificate.
 * i.e. if you get the exception below, specify connectionString with 127.0.0.1 (whatever matches the
 * node certificate) instead of localhost
 * Cause: java.security.cert.CertificateException: No name matching localhost found
 * {"coreId":"0xe5b56dbb00000002","local":"/127.0.0.1:64018","remote":"localhost/127.0.0.1:11207"}
 *
 */

@TestInstance(Lifecycle.PER_CLASS)
@Disabled
class QueryDataFrameCertificateIntegrationTest extends SparkOperationalTest {

  override def testName: String = super.testName

  override def sparkBuilderCustomizer(builder: SparkSession.Builder, params: Params): Unit = {
    builder
      .config("spark.couchbase.connectionString", params.connectionString)
      .config("spark.couchbase.implicitBucket", params.bucketName)
      .config("spark.couchbase.keyStorePath", "my.keystore")
      .config("spark.couchbase.keyStorePassword", "storepass")
      .config("spark.couchbase.keyStoreType", "jks")
      .config("spark.couchbase.security.trustCertificate", "ca.pem")
      .config("spark.couchbase.security.enableTls", "true")
  }

  @BeforeAll
  override def setup(): Unit = {
    infra = TestInfraBuilder() // set infra here, so that it is set even if prepareAirportSampleData fails
      .createBucketScopeAndCollection(testName)
      .connectToSpark(sparkBuilderCustomizer) // need to use sparkBuilderCustomizer
    infra.prepareAirportSampleData()
    spark = infra.spark
  }

  @Test
  def testReadDocumentsFromCollection(): Unit = {

    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, infra.params.scopeName)
      .option(QueryOptions.Collection, infra.params.collectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    assertEquals(4, airports.count)
    airports.foreach(row => {
      assertNotNull(row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("name"))
    })
  }

  @Test
  def testChangeIdFieldName(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.IdFieldName, "myIdFieldName")
      .option(QueryOptions.Scope, infra.params.scopeName)
      .option(QueryOptions.Collection, infra.params.collectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.foreach(row => {
      assertThrows(classOf[IllegalArgumentException], () => row.getAs[String]("__META_ID"))
      assertNotNull(row.getAs[String]("myIdFieldName"))
    })
  }

  @Test
  def testPushDownAggregationWithoutGroupBy(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, infra.params.scopeName)
      .option(QueryOptions.Collection, infra.params.collectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.createOrReplaceTempView("airports")

    val aggregates = spark.sql("select max(elevation) as el, min(runways) as run from airports")

    aggregates.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation =>
      assertTrue(p.toString().contains("MAX(`elevation`)"))
      assertTrue(p.toString().contains("MIN(`runways`)"))
    }

    assertEquals(204, aggregates.first().getAs[Long]("el"))
    assertEquals(2, aggregates.first().getAs[Long]("run"))
  }

  @Test
  def testPushDownAggregationWithGroupBy(): Unit = {
    val airports = spark.read
      .format("couchbase.query")
      .option(QueryOptions.Scope, infra.params.scopeName)
      .option(QueryOptions.Collection, infra.params.collectionName)
      .option(QueryOptions.ScanConsistency, QueryOptions.RequestPlusScanConsistency)
      .load()

    airports.createOrReplaceTempView("airports")

    val aggregates = spark.sql(
      "select max(elevation) as el, min(runways) as run, country from airports group by country"
    )

    aggregates.queryExecution.optimizedPlan.collect { case p: DataSourceV2ScanRelation =>
      assertTrue(p.toString().contains("country"))
      assertTrue(p.toString().contains("MAX(`elevation`)"))
      assertTrue(p.toString().contains("MIN(`runways`)"))
    }

    assertEquals(3, aggregates.count())
    assertEquals(183, aggregates.where("country = 'Austria'").first().getAs[Long]("el"))
    assertEquals(4, aggregates.where("country = 'Germany'").first().getAs[Long]("run"))
  }

}
