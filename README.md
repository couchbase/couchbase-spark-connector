# Couchbase Spark Connector

This repository contains the official [Apache Spark](https://spark.apache.org/) connector for [Couchbase Server](https://couchbase.com).

## Usage

The connector is available through the following coordinates:

 - **Group:** `com.couchbase.client`
 - **Artifact:** `spark-connector_2.12`
 - **Version:** `4.1.0`

If you are using SBT:

```
libraryDependencies += "com.couchbase.client" %% "spark-connector" % "4.1.0"
```

## Documentation
The official documentation, including a quickstart guide, can be found [here](https://docs.couchbase.com/spark-connector/current/index.html).

## Version Compatibility (from 4.X)
Couchbase Spark Connector 4.X changes the versioning model so that each 4.X release is compatible with multiple
Apache Spark 4.X versions.  See the release notes for the combinations that have been tested and are fully
supported.

## Version Compatibility (prior to 4.X)

Each minor release is targeted for a specific spark version and once released
branched away. Couchbase maintains bugfix releases for the branches where
appropriate, please see [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Ccom.couchbase.client.spark)
or [Spark Packages](http://spark-packages.org/package/couchbase/couchbase-spark-connector) for releases to download.

| Connector | Apache Spark | Couchbase Server |
|-----------|--------------|------------------|
| 3.5.x     | 3.5          | 7.x              |
| 3.3.x     | 3.3          | 6.x - 7.x        |
| 3.2.x     | 3.2          | 6.x - 7.x        |
| 3.1.x     | 3.1          | 6.x - 7.x        |
| 3.0.x     | 3.0          | 6.x - 7.x        |
| 2.4.x     | 2.4          | 5.x - 6.x        |
| 2.3.x     | 2.3          | 2.5.x - 6.x      |
| 2.2.x     | 2.2          | 2.5.x - 5.x      |
| 2.1.x     | 2.1          | 2.5.x - 4.x      |
| 2.0.x     | 2.0          | 2.5.x - 4.x      |
| 1.2.x     | 1.6          | 2.5.x - 4.x      |
| 1.1.x     | 1.5          | 2.5.x - 4.x      |
| 1.0.x     | 1.4          | 2.5.x - 4.x      |

## Development Tasks

Applying scalafmt (source code formatting) is done through `sbt scalafmtAll`.

Testing: `sbt test` and see `src/test/pyspark/README.md` for PySpark tests.  Note here you will need to use:

```
sbt assembly
# then set COUCHBASE_SPARK_JAR in src/test/pyspark/.env to:
#   <repo-root>/target/scala-2.13/spark-connector-assembly-<version>.jar
```

## License
Copyright 2015-2025 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See [the Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).