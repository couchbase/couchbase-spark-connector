# Couchbase Spark Connector

A library to integrate Couchbase Server with Spark in order to use it as a data source and target in various ways.

## Linking
You can link against this library (for Spark 3.0) in your program at the following coordinates:

```
groupId: com.couchbase.client
artifactId: spark-connector_2.12
version: 3.0.0
```

If you are using SBT:

```
libraryDependencies += "com.couchbase.client" %% "spark-connector" % "3.0.0"
```

## Documentation
The official documentation, including a quickstart guide, can be found [here](https://docs.couchbase.com/spark-connector/3.0/index.html).

## Version Compatibility

Each minor release is targeted for a specific spark version and once released
branched away. Couchbase maintains bugfix releases for the branches where
appropriate, please see [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Ccom.couchbase.client.spark)
or [Spark Packages](http://spark-packages.org/package/couchbase/couchbase-spark-connector) for releases to download.

| Connector | Apache Spark | Couchbase Server |
| --------- | ------------ | ---------------- |
| 3.0.x     | 3.0          | 5.x - 6.x        |
| 2.4.x     | 2.4          | 5.x - 6.x        |
| 2.3.x     | 2.3          | 2.5.x - 6.x      |
| 2.2.x     | 2.2          | 2.5.x - 5.x      |
| 2.1.x     | 2.1          | 2.5.x - 4.x      |
| 2.0.x     | 2.0          | 2.5.x - 4.x      |
| 1.2.x     | 1.6          | 2.5.x - 4.x      |
| 1.1.x     | 1.5          | 2.5.x - 4.x      |
| 1.0.x     | 1.4          | 2.5.x - 4.x      |

## Testing
Running the tests has the following requirements:

1. A Couchbase server (5.0 or higher) must be running on localhost.
2. The admin account must have username "Administrator" and password "password".
3. travel-sample example bucket must be installed (available in Admin UI: Settings -> Sample Buckets).
4. Query and data services must be running.
5. A primary index must exist on travel-sample ("CREATE PRIMARY INDEX on `travel-sample`;").
6. A bucket named "default" must exist with flushing enabled.

## License
Copyright 2015-2020 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See [the Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).