name := "spark-connector"

version := "3.5.3-SNAPSHOT"

organization := "com.couchbase.client"

crossScalaVersions := Seq("2.12.20", "2.13.16")

scalacOptions := Seq("-unchecked", "-deprecation")

publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

// Important: when changing this, lookup the current Jackson dependency for Spark and update the dependencyOverrides below
val sparkVersion = sys.props.get("spark.testVersion").getOrElse("3.5.6")
val operationalSdkVersion   = "1.8.3"
val enterpriseAnalyticsSdkVersion   = "1.0.0"
val dcpVersion   = "0.54.0"

scalacOptions += "-feature"

// Central Portal credentials
credentials += Credentials(Path.userHome / ".sbt" / "central_portal_credentials")

// Central Portal configuration
import xerial.sbt.Sonatype.sonatypeCentralHost
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

resolvers += Resolver.mavenLocal
resolvers += "Central Portal Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"

libraryDependencies ++= Seq(
  "org.apache.spark"     %% "spark-core"        % sparkVersion                     % Provided,
  "org.apache.spark"     %% "spark-sql"         % sparkVersion                     % Provided,
  "org.scala-lang"        % "scala-library"     % scalaVersion.value               % Provided,
  "com.couchbase.client" %% "scala-client"      % operationalSdkVersion,
  "com.couchbase.client"  % "couchbase-analytics-java-client"  % enterpriseAnalyticsSdkVersion,
  "com.couchbase.client"  % "dcp-client"        % dcpVersion,
  "net.aichler"           % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
  "org.testcontainers"    % "couchbase"         % "1.21.3"                         % Test,
  // For structured streaming testing
  "commons-codec"         % "commons-codec"     % "1.19.0"                         % Test
)

// The Java Enterprise Analytics SDK uses Jackson 2.19.1 but Spark 3.5.6 uses 2.15.2.
// We have to pin to the earlier version.
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2"
)

// Fix for JDK module system issues with Spark
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

// Enable forking to apply JVM arguments and add Java 17 compatibility options
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

homepage := Some(url("https://couchbase.com"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/couchbase/couchbase-spark-connector"),
    "git@github.com:couchbase/couchbase-spark-connector.git"
  )
)

developers := List(
  Developer(
    "daschl",
    "Michael Nitschinger",
    "michael.nitschinger@couchbase.com",
    url("https://github.com/daschl")
  ),
  Developer(
    "programmatix",
    "Graham Pople",
    "graham.pople@couchbase.com",
    url("https://github.com/programmatix")
  ),
  Developer("dnault", "David Nault", "david.nault@couchbase.com", url("https://github.com/dnault"))
)

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

publishMavenStyle := true

// Maven Central Portal publishing configuration
ThisBuild / publishTo := {
  if (isSnapshot.value)
    Some("Central Portal Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/")
  else
    sonatypePublishToBundle.value
}

// Exclude repositories from POM
pomIncludeRepository := { _ => false }

ThisBuild / assemblyMergeStrategy := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename("reactor.**" -> "com.couchbase.spark.deps.reactor.@1").inAll
)
