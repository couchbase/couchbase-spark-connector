# Couchbase Spark Connector

This repository contains the official [Apache Spark](https://spark.apache.org/) connector for [Couchbase Server](https://couchbase.com).

## Usage

The connector is available through the following coordinates:

 - **Group:** `com.couchbase.client`
 - **Artifact:** `spark-connector_2.12`
 - **Version:** `3.5.0` 

If you are using SBT:

```
libraryDependencies += "com.couchbase.client" %% "spark-connector" % "3.5.0"
```

## Documentation
The official documentation, including a quickstart guide, can be found [here](https://docs.couchbase.com/spark-connector/3.3/index.html).

## Version Compatibility

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

## License
Copyright 2015-2024 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See [the Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).