/*
 * Copyright (c) 2026 Couchbase, Inc.
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
package com.couchbase.spark.util

import com.couchbase.spark.config.{CertificateAuthOptions, CouchbaseConfig, Credentials}
import com.couchbase.spark.enterpriseanalytics.config.EnterpriseAnalyticsCredentials
import com.couchbase.spark.kv.{KeyValueOptions, KeyValueTableProvider}
import com.couchbase.spark.query.QueryReadConfig
import org.apache.spark.SparkConf
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNull, assertTrue}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class RedactionTest {

  private def redact(s: String): String = Redaction.connectionIdentifier(s)

  @Test
  def passwordIsRedacted(): Unit = {
    assertEquals("couchbase://user:*****@host", redact("couchbase://user:hunter2@host"))
  }

  @Test
  def hostsAndPortsArePreserved(): Unit = {
    assertEquals(
      "couchbases://user:*****@host1:11207,host2",
      redact("couchbases://user:hunter2@host1:11207,host2")
    )
  }

  @Test
  def sensitiveParamsAreRedacted(): Unit = {
    assertEquals(
      "couchbase://user:*****@host?spark.couchbase.timeout.kvTimeout=10s" +
        "&spark.ssl.keyStorePassword=*****&spark.couchbase.keyStorePassword=*****&someToken=*****",
      redact(
        "couchbase://user:hunter2@host?spark.couchbase.timeout.kvTimeout=10s" +
          "&spark.ssl.keyStorePassword=kspw&spark.couchbase.keyStorePassword=certpw&someToken=t"
      )
    )
  }

  @Test
  def paramsWithoutCredentials(): Unit = {
    assertEquals(
      "couchbase://host?spark.ssl.keyStorePassword=*****",
      redact("couchbase://host?spark.ssl.keyStorePassword=kspw")
    )
  }

  @Test
  def passwordContainingQuestionMarkIsRedacted(): Unit = {
    // As with the SDK's ConnectionString, the user part runs to the first '@'.
    assertEquals("couchbase://user:*****@host", redact("couchbase://user:pa?ss@host"))
  }

  @Test
  def noCredentials(): Unit = {
    assertEquals("couchbase://host:8091", redact("couchbase://host:8091"))
  }

  @Test
  def usernameOnlyIsKept(): Unit = {
    assertEquals("couchbase://user@host", redact("couchbase://user@host"))
  }

  @Test
  def staticIdentifierIsUnchanged(): Unit = {
    assertEquals("test", redact("test"))
    assertEquals(Some("test"), Redaction.connectionIdentifier(Some("test")))
    assertEquals(None, Redaction.connectionIdentifier(None: Option[String]))
  }

  @Test
  def nullIsUnchanged(): Unit = {
    assertNull(redact(null))
  }

  @Test
  def sensitivePropertiesAreRedacted(): Unit = {
    assertEquals(
      Seq("io.numKvConnections" -> "4", "security.trustStorePassword" -> Redaction.Redacted),
      Redaction.properties(
        Seq("io.numKvConnections" -> "4", "security.trustStorePassword" -> "tspw")
      )
    )
  }

  @Test
  def credentialClassesRedactPasswords(): Unit = {
    assertEquals("Credentials(user,*****)", Credentials("user", "hunter2").toString)
    assertEquals(
      "EnterpriseAnalyticsCredentials(user,*****)",
      EnterpriseAnalyticsCredentials("user", "hunter2").toString
    )
    assertEquals(
      "CertificateAuthOptions(/ks.jks,*****,JKS)",
      CertificateAuthOptions("/ks.jks", "hunter2", "JKS").toString
    )
  }

  private val Identifier =
    "couchbases://user:hunter2@host?spark.ssl.enabled=true&spark.ssl.keyStorePassword=kspw" +
      "&spark.couchbase.implicitBucket=travel-sample"

  private def assertNoSecrets(s: String): Unit = {
    Seq("hunter2", "kspw", "tspw").foreach(secret =>
      assertFalse(s.contains(secret), s"'$secret' found in: $s")
    )
  }

  @Test
  def couchbaseConfigFromDynamicIdentifierToStringHasNoSecrets(): Unit = {
    val config = CouchbaseConfig(new SparkConf(false), Some(Identifier))

    // Check the secrets did make it into the config, so the test is meaningful.
    assertEquals(Some("hunter2"), config.credentials.map(_.password))
    assertEquals(Some("kspw"), config.sparkSslOptions.keystorePassword)

    assertNoSecrets(config.toString)
  }

  @Test
  def couchbaseConfigFromSparkConfToStringHasNoSecrets(): Unit = {
    val sparkConf = new SparkConf(false)
      .set("spark.couchbase.connectionString", "couchbase://host")
      .set("spark.couchbase.username", "user")
      .set("spark.couchbase.password", "hunter2")
      .set("spark.couchbase.security.trustStorePassword", "tspw")
    val config = CouchbaseConfig(sparkConf, None)

    assertEquals(Some("hunter2"), config.credentials.map(_.password))
    assertTrue(config.properties.contains("security.trustStorePassword" -> "tspw"))

    assertNoSecrets(config.toString)
  }

  @Test
  def keyValueWriteConfigToStringHasNoSecrets(): Unit = {
    // This is the object logged at INFO for every partition of a KV DataFrame write.
    val conf = CouchbaseConfig(new SparkConf(false), Some(Identifier))
    val writeConfig = new KeyValueTableProvider().writeConfig(
      Map(KeyValueOptions.ConnectionIdentifier -> Identifier).asJava,
      conf
    )
    assertEquals(Some(Identifier), writeConfig.connectionIdentifier)

    val s = writeConfig.toString
    assertNoSecrets(s)
    assertTrue(s.contains("couchbases://user:*****@host?"), s)
    assertTrue(s.startsWith("KeyValueWriteConfig(travel-sample,"), s)
  }

  @Test
  def queryReadConfigToStringHasNoSecrets(): Unit = {
    val readConfig = QueryReadConfig(
      "travel-sample",
      None,
      None,
      "__META_ID",
      "__META_CAS",
      outputCas = false,
      None,
      "notBounded",
      None,
      pushDownAggregate = true,
      Some(Identifier),
      None
    )
    assertNoSecrets(readConfig.toString)
  }
}
