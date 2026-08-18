/** A tested Spark profile: a Spark release the connector is built and tested against, together with
  * the Hadoop and Jackson versions that that Spark release actually ships.
  *
  * The Hadoop version is required as we need to explicitly depend on it during testing.
  * The Jackson version because we depend on the Couchbase Enterprise Analytics SDK which has its own unshaded Jackson
  *    dependency, and we need to ensure the Spark and EA Jackson versions are compatible.  Generally we pin to the
  *    lowest, which is generally Spark.
  * Jackson annotations for the same reason.  As for Jackson 2.20 it's a separate version from the main databind version.
  */
case class SparkProfile(
    spark: String,
    hadoop: String,
    jackson: String,
    jacksonAnnotations: String
)

/** Where the numbers come from, for each Spark version:
  *   - spark / hadoop / jackson:
  *     https://repo1.maven.org/maven2/org/apache/spark/spark-parent_2.13/<v>/spark-parent_2.13-<v>.pom
  * -> `<hadoop.version>` and `<fasterxml.jackson.version>`
  *   - jacksonAnnotations:
  *     https://repo1.maven.org/maven2/com/fasterxml/jackson/jackson-bom/<jackson>/jackson-bom-<jackson>.pom
  * -> `<jackson.version.annotations>`
  */
object SparkProfiles {

  // https://endoflife.date/apache-spark
  val all: Map[String, SparkProfile] = Map(
    // 4.3 not available yet (as of 19th Aug '26)

    // 4.2 supported by Apache until 11 Jan 2028 (https://endoflife.date/apache-spark)
    "4.2" -> SparkProfile(
      spark = "4.2.0",
      hadoop = "3.5.0",
      jackson = "2.21.2",
      jacksonAnnotations = "2.21"
    ),

    // 4.1 supported by Apache until 11 Jun 2027 (https://endoflife.date/apache-spark)
    "4.1" -> SparkProfile(
      spark = "4.1.3",
      hadoop = "3.4.2",
      jackson = "2.21.2",
      jacksonAnnotations = "2.21"
    )

    // Spark 4.0 is deliberately absent:
    //
    //  1. It reaches end of support on 2026-11-23 (https://endoflife.date/apache-spark)
    //  2. It ships Jackson 2.18.2, while the Enterprise Analytics SDK wants 2.21.4.

    // Spark 3.5 is not possible from this 4.X connector due to changes at the Spark ABI.
  )


  /** The profile the released artifact is built against.
    *
    * Because the Spark dependencies are Provided, users can and will run against later Spark versions.  We document
    * what combinations are tested and officially supported in the relnotes.
    */
  val releaseKey = "4.1"

  val keys: Seq[String] = all.keys.toSeq.sorted

  /** Selects the profile to build and test against:
    * {{{
    * sbt -Dspark.profile=4.2 testPublished
    * }}}
    * Defaults to [[releaseKey]].
    */
  val activeKey: String = sys.props.getOrElse("spark.profile", releaseKey)

  val active: SparkProfile = all.getOrElse(
    activeKey,
    sys.error(s"Unknown Spark profile '$activeKey'. Known profiles: " + keys.mkString(", "))
  )

  val activeSparkVersion: String = active.spark

  /** The Spark *runtime* the cross-runtime test project runs against, which is independent of the
    * profile the connector was compiled against:
    * {{{
    * sbt -Dspark.runtime=4.2 crossRuntimeTests/test
    * }}}
    * This is what proves a connector built for one Spark release still works on a later one -
    * the connector classes stay fixed and only the Spark on the test classpath changes.
    * Defaults to [[activeKey]], i.e. compile and run against the same release.
    */
  val runtimeKey: String = sys.props.getOrElse("spark.runtime", activeKey)

  val runtime: SparkProfile = all.getOrElse(
    runtimeKey,
    sys.error(s"Unknown Spark runtime profile '$runtimeKey'. Known profiles: " + keys.mkString(", "))
  )

  /** Shown on build load so a CI matrix job's output is unambiguous about what it built. */
  def describe: String =
    s"Spark profile $activeKey: spark $activeSparkVersion, hadoop ${active.hadoop}, " +
      s"jackson ${active.jackson} (annotations ${active.jacksonAnnotations})" +
      (if (runtimeKey != activeKey) s" | cross-runtime target: spark ${runtime.spark}" else "")
}
