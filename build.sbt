name := "spark-connector"

version := "2.0.0-SNAPSHOT"

organization := "com.couchbase.client"

scalaVersion := "2.12.14"

scalacOptions := Seq("-unchecked", "-deprecation")

val sparkVersion = sys.props.get("spark.testVersion").getOrElse("3.0.3")
val sdkVersion = "1.2.1-SNAPSHOT"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scala-lang" % "scala-library" % scalaVersion.value % Provided,
  "com.couchbase.client" %% "scala-client" % sdkVersion,
  "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
  "org.testcontainers" % "couchbase" % "1.16.0" % Test
)