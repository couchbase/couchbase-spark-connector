import sbt.Keys.publishArtifact
import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

import scala.io.Source

makePomConfiguration := makePomConfiguration.value.withConfigurations(
  Configurations.defaultMavenConfigurations
)
conflictManager := ConflictManager.default

lazy val sparkVersion = "3.0.1"
lazy val scalaLanguageVersion = "2.12.12"

Project.inConfig(Test)(baseAssemblySettings)

lazy val commonSettings = Seq(
  organizationName := "Couchbase, Inc.",
  organization := "com.couchbase.client",
  version := {
    val v = Source.fromFile("version", "UTF-8").mkString
    if (!v.matches("""^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(-SNAPSHOT)?$""")) {
      throw new RuntimeException("Invalid version format!")
    }
    v
  },
  scalaVersion := scalaLanguageVersion,
  logLevel in test := Level.Debug,
  logLevel in assembly := Level.Debug,
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  sources in (Compile, doc) := Seq.empty,
  publishArtifact in (Compile, packageDoc) := false,
  publishTo := Some(
    "Artifactory Realm".at(s"https://artifactory.enliven.systems/artifactory/sbt-dev-local/")
  ),
  resolvers ++= Seq(
    "Maven Central".at("https://repo1.maven.org/maven2/")
  ),
  assemblyShadeRules in assembly := Seq(
    ShadeRule
      .rename("com.couchbase.client.java.**" -> "shaded.com.couchbase.client.java.@1")
      .inLibrary("com.couchbase.client" % "java-client" % "2.7.16")
      .inProject,
    ShadeRule
      .rename("com.couchbase.client.core.**" -> "shaded.com.couchbase.client.core.@1")
      .inAll,
    ShadeRule
      .rename("com.couchbase.client.encryption.**" -> "shaded.com.couchbase.client.encryption.@1")
      .inAll
  ),
  assemblyExcludedJars in assembly := {
    val classPath = (fullClasspath in assembly).value
    classPath.filter {
      e => !List("core-io-1.7.16.jar", "java-client-2.7.16.jar").contains(e.data.getName)
    }
  },
  assemblyMergeStrategy in assembly := {
    case PathList("com.couchbase.client.core.properties") => new ShadedMergeStrategy()
    case PathList("com.couchbase.client.java.properties") => new ShadedMergeStrategy()
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  publishArtifact in (Test, packageBin) := true,
  publishArtifact in (Test, packageSrc) := true,
  publishArtifact in (Compile, packageDoc) := false,
  parallelExecution := false,
  test in assembly := {},
  artifact in (Compile, assembly) := {
    val art = (artifact in (Compile, assembly)).value
    art.withClassifier(Some("shaded"))
  },
  addArtifact(artifact in (Compile, assembly), assembly),
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-library" % scalaLanguageVersion,
    "com.couchbase.client" % "java-client" % "2.7.16",
    "com.couchbase.client" % "dcp-client" % "0.31.0",
    "io.reactivex" %% "rxscala" % "0.27.0",
    "org.apache.logging.log4j" % "log4j-api" % "2.13.3",
    "org.scalatestplus" %% "junit-4-13" % "3.2.3.0" % "test",
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.scalatest" %% "scalatest" % "3.2.3" % "test",
    "junit" % "junit" % "4.13.1" % "test",
    "com.nrinaudo" %% "kantan.csv" % "0.6.1"
  )
)

lazy val core = (project in file("spark-connector")).settings(commonSettings: _*).settings(
  name := "spark-connector"
)

lazy val root = (project in file(".")).settings(commonSettings: _*).aggregate(core).dependsOn(
  core % "test->test;compile->compile"
)
