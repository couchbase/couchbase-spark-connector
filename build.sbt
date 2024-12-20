name := "spark-connector"

version := "3.5.2"

organization := "com.couchbase.client"

crossScalaVersions := Seq("2.12.20", "2.13.15")

scalacOptions := Seq("-unchecked", "-deprecation")

publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

val sparkVersion = sys.props.get("spark.testVersion").getOrElse("3.5.3")
val sdkVersion   = "1.7.6"
val dcpVersion   = "0.52.0"

scalacOptions += "-feature"

credentials += Credentials("Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  sys.env.getOrElse("SONATYPE_USERNAME", ""),
  sys.env.getOrElse("SONATYPE_PASSWORD", ""))
credentials += Credentials("Sonatype Nexus Repository Manager",
  "ossrh",
  sys.env.getOrElse("SONATYPE_USERNAME", ""),
  sys.env.getOrElse("SONATYPE_PASSWORD", ""))

resolvers += Resolver.mavenLocal
resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.apache.spark"     %% "spark-core"        % sparkVersion                     % Provided,
  "org.apache.spark"     %% "spark-sql"         % sparkVersion                     % Provided,
  "org.scala-lang"        % "scala-library"     % scalaVersion.value               % Provided,
  "com.couchbase.client" %% "scala-client"      % sdkVersion,
  "com.couchbase.client"  % "dcp-client"        % dcpVersion,
  "net.aichler"           % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
  "org.testcontainers"    % "couchbase"         % "1.20.4"                         % Test,
  // For structured streaming
  "commons-codec"         % "commons-codec"     % "1.17.1"                        //   % Test
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

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

ThisBuild / assemblyMergeStrategy := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename("reactor.**" -> "com.couchbase.spark.deps.reactor.@1").inAll
)
