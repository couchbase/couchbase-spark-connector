name := "spark-connector"

version := "3.3.7"

organization := "com.couchbase.client"

scalaVersion := "2.12.20"

scalacOptions := Seq("-unchecked", "-deprecation")

publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

val sparkVersion = sys.props.get("spark.testVersion").getOrElse("3.3.4")
val sdkVersion   = "1.9.1"
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
  "com.couchbase.client" %% "scala-client"      % sdkVersion,
  "com.couchbase.client"  % "dcp-client"        % dcpVersion,
  "net.aichler"           % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
  "org.testcontainers"    % "couchbase"         % "1.21.1"                         % Test,
  // For structured streaming testing
  "commons-codec"         % "commons-codec"     % "1.17.1"                         % Test
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
