# Couchbase Spark Connector

A library to integrate Couchbase Server with Spark in order to use it as a data source and target in various ways.

## Linking
You can link against this library (for Spark 1.6) in your program at the following coordinates:

```
groupId: com.couchbase.client
artifactId: spark-connector_2.10
version: 1.2.1
```

If you are using SBT:

```
libraryDependencies += "com.couchbase.client" %% "spark-connector" % "1.2.1"
```

## Documentation
The official documentation, including a quickstart guide can be found [here](http://docs.couchbase.com).

## Version Compatibility

Each minor release is targeted for a specific spark version and once released
branched away. Couchbase maintains bugfix releases for the branches where
appropriate, please see [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Ccom.couchbase.client.spark) 
or [Spark Packages](http://spark-packages.org/package/couchbase/couchbase-spark-connector) for releases to download.

| Connector | Spark |
| --------- | ----- |
| 1.2.x     | 1.6   |
| 1.1.x     | 1.5   |
| 1.0.x     | 1.4   |


## License
Copyright 2015,2016 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See [the Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).