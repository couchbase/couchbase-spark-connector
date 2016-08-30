name := "spark-connector"

organization := "com.couchbase.client"

version := "1.3.0"

description := "Official Couchbase Spark Connector"

organizationHomepage := Some(url("http://couchbase.com"))

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8", "2.10.6")
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
scalacOptions += "-target:jvm-1.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
  "com.couchbase.client" % "java-client" % "2.3.2",
  "io.reactivex" %% "rxscala" % "0.26.1",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.apache.logging.log4j" % "log4j-api" % "2.2"
)

resolvers += Resolver.mavenLocal

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

publishMavenStyle := true

publishArtifact in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

pomExtra := (
  <url>https://github.com/couchbaselabs/couchbase-spark-connector</url>
  <scm>
    <url>git@github.com:couchbaselabs/couchbase-spark-connector.git</url>
    <connection>scm:git:git@github.com:couchbaselabs/couchbase-spark-connector.git</connection>
  </scm>
  <developers>
    <developer>
      <id>daschl</id>
      <name>Michael Nitschinger</name>
      <email>michael.nitschinger@couchbase.com</email>
    </developer>
  </developers>
)

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.rename
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

fork := true
