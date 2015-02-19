name := "couchbase-spark-connector"

organization := "com.couchbase.client"

description := "Official Couchbase Spark Connector"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.11.5", "2.10.4")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1",
  "com.couchbase.client" % "java-client" % "2.1.1-SNAPSHOT",
  "io.reactivex" %% "rxscala" % "0.23.1",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
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