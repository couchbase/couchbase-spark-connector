name := "docs"

version := "0.1"

scalaVersion := "2.12.14"

Compile / scalaSource := baseDirectory.value / "modules" / "ROOT" / "examples"
Compile / javaSource := baseDirectory.value / "modules" / "ROOT" / "examples"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.3",
  "org.apache.spark" %% "spark-sql" % "3.0.3",
  "com.couchbase.client" %% "spark-connector" % "3.0.0-SNAPSHOT"

)

resolvers += Resolver.mavenLocal