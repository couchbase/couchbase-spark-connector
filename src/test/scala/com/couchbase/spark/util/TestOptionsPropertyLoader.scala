package com.couchbase.spark.util

import java.util.Properties
import scala.util.Try

trait CouchbaseClusterSettings {
  def connectionString: String
  def username: String
  def password: String
  def tlsEnabled: Boolean
  def tlsInsecure: Boolean
  def clusterType: String
}

class TestOptionsPropertyLoader extends CouchbaseClusterSettings {
  private val properties = new Properties()

  private val propertiesFile = List("integration.local.properties", "integration.properties")
    .map(fileName => Option(getClass.getResourceAsStream("/" + fileName)))
    .find(_.isDefined)
    .getOrElse(throw new RuntimeException("Neither 'integration.local.properties' nor 'integration.properties' found"))
    .get

  properties.load(propertiesFile)

  def connectionString: String = properties.getProperty("connectionString", "localhost")
  def username: String = properties.getProperty("username", "Administrator")
  def password: String = properties.getProperty("password", "password")
  def tlsEnabled: Boolean = properties.getProperty("tlsEnabled", "false").toBoolean
  def tlsInsecure: Boolean = properties.getProperty("tlsInsecure", "false").toBoolean
  def clusterType: String = properties.getProperty("clusterType", "operational")
}