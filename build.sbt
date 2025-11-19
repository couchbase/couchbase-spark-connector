name := "spark-connector"

version := "3.5.4"

organization := "com.couchbase.client"

// Latest: https://www.scala-lang.org/download/all.html
crossScalaVersions := Seq("2.12.20", "2.13.17")

scalacOptions := Seq("-unchecked", "-deprecation")

publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishM2Configuration := publishM2Configuration.value.withOverwrite(true)

// Latest: https://spark.apache.org/downloads.html
// Important: when changing this, lookup the current Jackson dependency for Spark and update the dependencyOverrides below
// Find it here: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.13/<spark version>
val sparkVersion = sys.props.get("spark.testVersion").getOrElse("3.5.7")
// Latest here https://docs.couchbase.com/scala-sdk/current/project-docs/sdk-release-notes.html
// Need https://jira.issues.couchbase.com/browse/SCBC-502 to be fixed before can pick up 3.X series
val operationalSdkVersion   = "1.8.3"
// Latest here https://docs.couchbase.com/java-analytics-sdk/current/project-docs/analytics-sdk-release-notes.html
val enterpriseAnalyticsSdkVersion   = "1.0.0"
// Latest here https://mvnrepository.com/artifact/com.couchbase.client/dcp-client
val dcpVersion   = "0.56.0"

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
  "commons-codec"         % "commons-codec"     % "1.20.0"                         % Test
)

// The Java Enterprise Analytics 1.0.0 SDK uses Jackson 2.19.1 but Spark 3.5.7 uses 2.15.2.
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
  // Drop manifests, signatures, and index files
  case PathList("META-INF", "MANIFEST.MF")                                   => MergeStrategy.discard
  case PathList("META-INF", "INDEX.LIST")                                    => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.endsWith(".sf"))  => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.endsWith(".rsa")) => MergeStrategy.discard

  // Duplicate legal/notice files (e.g., FastDoubleParser-NOTICE) can be safely discarded
  case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.startsWith("notice"))  => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.startsWith("license")) => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.contains("fastdoubleparser-notice")) => MergeStrategy.discard

  // Drop Java 9+ multi-release module descriptors which commonly collide
  case PathList("META-INF", "versions", _*) => MergeStrategy.discard
  case "module-info.class"                   => MergeStrategy.discard
  case x if x.endsWith("module-info.class")  => MergeStrategy.discard

  // Keep only first for specific known duplicate properties
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first

  // Merge Java service provider files
  case PathList("META-INF", "services", _*) => MergeStrategy.concat

  // Fallback to previous strategy
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename("reactor.**" -> "com.couchbase.spark.deps.reactor.@1").inAll
)
