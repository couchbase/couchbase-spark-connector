name := "spark-connector"

version := "4.0.2"

organization := "com.couchbase.client"

// Latest: https://www.scala-lang.org/download/all.html
crossScalaVersions := Seq("2.13.18")
scalaVersion := "2.13.18"

scalacOptions := Seq("-unchecked", "-deprecation")

publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishM2Configuration := publishM2Configuration.value.withOverwrite(true)

// # Dependency: Spark
// Latest: https://spark.apache.org/downloads.html
val sparkVersion        = SparkProfiles.activeSparkVersion
// Need to depend on Hadoop explicitly for testing.
val hadoopClientVersion = SparkProfiles.active.hadoop

// Make the active profile obvious in CI logs, so a matrix job's output is unambiguous.
onLoadMessage := SparkProfiles.describe

val sparkProfiles = taskKey[Unit]("Print the available Spark profile keys, one per line.")
sparkProfiles := SparkProfiles.keys.foreach(println)

val sparkReleaseProfile = taskKey[Unit]("Print the Spark profile the release is built against.")
sparkReleaseProfile := println(SparkProfiles.releaseKey)

val buildVersions = taskKey[Unit]("Print the Spark, Hadoop, Jackson and Enterprise Analytics SDK versions being built against.")
buildVersions := {
  println("[cols=\"1,1\"]")
  println("|===")
  println("| Dependency | Version")
  println()
  println(s"| Scala\n| ${scalaVersion.value}")
  println()
  println(s"| Couchbase operational SDK\n| ${operationalSdkVersion}")
  println()
  println(s"| Couchbase Enterprise Analytics SDK\n| ${enterpriseAnalyticsSdkVersion}")
  println()
  println(s"| Jackson\n| ${SparkProfiles.active.jackson}")
  println()
  println(s"| Jackson annotations\n| ${SparkProfiles.active.jacksonAnnotations}")
  println("|===")
  println()
  println("Spark versions actively tested:")
  println()
  println("[cols=\"1,1,1,1\"]")
  println("|===")
  println("| Profile | Spark | Jackson | Jackson annotations")
  println()
  SparkProfiles.keys.foreach { key =>
    val profile = SparkProfiles.all(key)
    println(s"| $key\n| ${profile.spark}\n| ${profile.jackson}\n| ${profile.jacksonAnnotations}")
    println()
  }
  println("|===")
}

// # Dependency: Operational Couchbase SDK
// Latest here https://docs.couchbase.com/scala-sdk/current/project-docs/sdk-release-notes.html
// https://jira.issues.couchbase.com/browse/SCBC-502 is still open (3.x jars bundle unshaded
// upickle classes), so we shade upickle ourselves below rather than waiting on it.
val operationalSdkVersion   = "3.13.0"

// # Dependency: Analytics Couchbase SDK
// Latest here https://docs.couchbase.com/java-analytics-sdk/current/project-docs/analytics-sdk-release-notes.html
// When updating this, also update the "The Java Enterprise Analytics 1.1.0 SDK" comment below and possibly the Jackson version.
val enterpriseAnalyticsSdkVersion   = "1.1.0"

// See SparkProfile comments for why we pin the Jackson version.
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind"   % SparkProfiles.active.jackson,
  "com.fasterxml.jackson.core" % "jackson-core"       % SparkProfiles.active.jackson,
  "com.fasterxml.jackson.core" % "jackson-annotations" % SparkProfiles.active.jacksonAnnotations
)

// Latest here https://mvnrepository.com/artifact/com.couchbase.client/dcp-client
val dcpVersion   = "0.57.0"

scalacOptions += "-feature"

// Central Portal credentials
credentials += Credentials(Path.userHome / ".sbt" / "central_portal_credentials")

// Central Portal configuration
import xerial.sbt.Sonatype.sonatypeCentralHost
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

resolvers += "Central Portal Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"

libraryDependencies ++= Seq(
  "org.apache.spark"     %% "spark-core"        % sparkVersion                     % Provided,
  "org.apache.spark"     %% "spark-sql"         % sparkVersion                     % Provided,
  "org.scala-lang"        % "scala-library"     % scalaVersion.value               % Provided,
  "com.couchbase.client" %% "scala-client"      % operationalSdkVersion,
  "com.couchbase.client"  % "couchbase-analytics-java-client"  % enterpriseAnalyticsSdkVersion,
  "com.couchbase.client"  % "dcp-client"        % dcpVersion,
  "net.aichler"           % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
  "org.testcontainers"    % "couchbase"         % "1.21.4"                         % Test,
  // For structured streaming testing
  "commons-codec"         % "commons-codec"     % "1.22.1"                         % Test,
  // Needed for SparkSession to work in tests; see comment on hadoopClientVersion above.
  "org.apache.hadoop"     % "hadoop-client-api"     % hadoopClientVersion          % Test,
  "org.apache.hadoop"     % "hadoop-client-runtime" % hadoopClientVersion          % Test
)

val commonTestSettings: Seq[Setting[_]] = Seq(
  // Fix for JDK module system issues with Spark
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  // Enable forking to apply JVM arguments and add Java 17 compatibility options
  Test / fork := true,
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
)

commonTestSettings

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

  case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.contains("notice"))  => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) if xs.exists(_.toLowerCase.contains("license")) => MergeStrategy.concat

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
  ShadeRule.rename("reactor.**" -> "com.couchbase.spark.deps.reactor.@1").inAll,
  // scala-client bundles its own copy of upickle unshaded (SCBC-502); shade here.
  ShadeRule
    .rename("upickle.**" -> "com.couchbase.spark.deps.upickle.@1")
    .inLibrary("com.couchbase.client" % "scala-client_2.13" % operationalSdkVersion)
)


// ---------------------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------
// Test projects that run the suite against a Spark runtime chosen independently of the Spark the
// connector was built against.
//
// `sbt test` on the main project compiles the connector against a Spark profile *and* tests it
// against that same Spark. That answers "does the source still work with Spark X", but not the
// question that matters to users: "does what we shipped, built against Spark X, work on their
// Spark Y". These projects answer that, each consuming the connector a different way:
//
//   crossRuntimeTests - the compiled classes    (fastest; no packaging involved)
//   publishedTests    - the published thin jar  (exercises the POM and the unshaded dep graph)
val sparkRuntimeTestSettings: Seq[Setting[_]] = commonTestSettings ++ Seq(
  publish / skip := true,
  // Must match the root project, or `%%` resolves a _2.12 artifact.
  scalaVersion := (LocalRootProject / scalaVersion).value,
  // Reuse the main project's tests verbatim rather than maintaining a second copy.
  Test / unmanagedSourceDirectories   := Seq((LocalRootProject / Test / scalaSource).value),
  Test / unmanagedResourceDirectories := Seq((LocalRootProject / Test / resourceDirectory).value),
  // The tests load fixtures via paths relative to the working directory, e.g.
  // `.json("src/test/resources/airports.json")` in TestInfra, so the forked JVM has to run from
  // the repo root rather than from the subproject's directory.
  Test / baseDirectory := (LocalRootProject / baseDirectory).value,
  libraryDependencies ++= Seq(
    "org.apache.spark"   %% "spark-core"            % SparkProfiles.runtime.spark  % Test,
    "org.apache.spark"   %% "spark-sql"             % SparkProfiles.runtime.spark  % Test,
    "org.apache.hadoop"   % "hadoop-client-api"     % SparkProfiles.runtime.hadoop % Test,
    "org.apache.hadoop"   % "hadoop-client-runtime" % SparkProfiles.runtime.hadoop % Test,
    "net.aichler"         % "jupiter-interface"     % JupiterKeys.jupiterVersion.value % Test,
    "org.testcontainers"  % "couchbase"             % "1.21.4" % Test,
    "commons-codec"       % "commons-codec"         % "1.22.0" % Test
  ),
  // Jackson must match the *runtime* Spark, not the one the connector was compiled against.
  dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-databind"    % SparkProfiles.runtime.jackson,
    "com.fasterxml.jackson.core" % "jackson-core"        % SparkProfiles.runtime.jackson,
    "com.fasterxml.jackson.core" % "jackson-annotations" % SparkProfiles.runtime.jacksonAnnotations
  )
)

// Runs the suite against the connector classes already compiled against SparkProfiles.active -
// it does not rebuild them - with a different Spark on the test classpath.
//
//   sbt -Dspark.runtime=4.2 crossRuntimeTests/test
//
// Validates linkage and behaviour of the compiled classes. It does *not* exercise packaging:
// no POM, no shading, no META-INF/services merging. See publishedTests for that.
lazy val crossRuntimeTests = (project in file("cross-runtime-tests"))
  .dependsOn(LocalRootProject)
  .settings(sparkRuntimeTestSettings)
  .settings(name := "spark-connector-cross-runtime-tests")

// Runs the suite against the *published* thin jar, resolved from the local Maven repository by
// coordinate rather than as an internal project dependency. This is what users of Maven Central
// and `--packages` actually consume.
//
// Because it resolves by coordinate, the root project must be published to ~/.m2 first:
//
//   sbt -Dspark.runtime=4.2 testPublished          # publishes, then tests (see the alias below)
//   sbt publishM2                                  # or do it by hand, then:
//   sbt -Dspark.runtime=4.2 publishedTests/testOnly com.couchbase.spark.kv.LookupInRDDIntegrationTest
//
// publishM2 rather than publishLocal on purpose: publishLocal writes Ivy metadata to ~/.ivy2,
// which would not exercise the POM. The POM is a large part of what we are testing here.
lazy val publishedTests = (project in file("published-tests"))
  .settings(sparkRuntimeTestSettings)
  .settings(
    name := "spark-connector-published-tests",
    resolvers += Resolver.mavenLocal,
    libraryDependencies +=
      (LocalRootProject / organization).value %% "spark-connector" %
        (LocalRootProject / version).value % Test
  )

addCommandAlias("testPublished", "publishM2; publishedTests/test")
