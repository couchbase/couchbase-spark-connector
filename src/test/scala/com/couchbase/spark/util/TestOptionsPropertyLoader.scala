package com.couchbase.spark.util

import java.util.Properties
import scala.util.Try

class TestOptionsPropertyLoader {
  private val properties = new Properties()

  private val propertiesFile = List("integration.local.properties", "integration.properties")
    .map(fileName => Try(getClass.getResourceAsStream("/" + fileName)))
    .find(_.isSuccess)
    .getOrElse(throw new RuntimeException("Neither 'integration.local.properties' nor 'integration.properties' found"))

  properties.load(propertiesFile.get)

  def connectionString: String = properties.getProperty("connectionString", "localhost")
  def username: String = properties.getProperty("username", "Administrator")
  def password: String = properties.getProperty("password", "password")
  def tlsEnabled: Boolean = properties.getProperty("tlsEnabled", "false").toBoolean
  def clusterType: String = properties.getProperty("clusterType", "operational")
}