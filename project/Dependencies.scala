import net.aichler.jupiter.sbt.Import.JupiterKeys
import sbt._

object Dependencies {
  // Versions
  val scalaCoreVersion = "2.12.11"
  val scalaVersions = Seq("2.12.11")
  val couchbaseSdkVersion = "1.1.1-SNAPSHOT"
  val sparkVersion = "3.0.1"
  val scalaTestVersion = "3.2.2"

  // Libraries
  val couchbaseSdk = "com.couchbase.client" %% "scala-client" % couchbaseSdkVersion
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val java8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1"
  val couchbaseDcp = "com.couchbase.client" % "dcp-client" % "0.31.0"

  // Testing
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  val jupiterInterface = "net.aichler" % "jupiter-interface" % "0.8.3" % "test"
  val awaitilityTest = "org.awaitility" % "awaitility" % "4.0.3" % "test"
  val testcontainerTest = "org.testcontainers" % "testcontainers" % "1.14.3" % "test"
  val mockTest = "com.github.Couchbase" % "CouchbaseMock" % "73e493d259" % "test"

  // Projects
  val coreDependencies = Seq(couchbaseSdk, sparkCore, sparkSql, java8Compat, couchbaseDcp)
  val testDependencies = Seq(scalaTest, jupiterInterface, awaitilityTest, testcontainerTest, mockTest)

}