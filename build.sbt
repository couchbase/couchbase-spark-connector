import Dependencies._

ThisBuild / organization := "com.couchbase.client"
ThisBuild / version      := "3.0.0-SNAPSHOT"
ThisBuild / scalaVersion := scalaCoreVersion

lazy val root = (project in file("."))
  .settings(
    name := "spark-connector",
    libraryDependencies ++= coreDependencies,
    libraryDependencies ++= testDependencies,
  )

resolvers += Resolver.mavenLocal
resolvers += Resolver.jcenterRepo
resolvers += "jitpack" at "https://jitpack.io"