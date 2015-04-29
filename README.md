# Couchbase Spark Connector

A library to integrate Couchbase Server with Spark in order to use it as a data source and target in various ways.

## Requirements
This library requires Spark 1.3.+ and is currently available as a developer preview. It is not intended for production
use yet.

## Linking
You can link against this library (for Spark 1.3+) in your program at the following coordinates:

```
groupId: com.couchbase.client
artifactId: spark-connector_2.10
version: 1.0.0-dp
```

If you are using SBT:

```
libraryDependencies += "com.couchbase.client" %% "spark-connector" % "1.0.0-dp"
```

Since right now only a developer preview is available, you also need to include the Couchbase Maven Repository. Once
a GA release ships, the artifacts will be available from Maven Central.

```
resolvers += "Couchbase Repository" at "http://files.couchbase.com/maven2/"
```

## Quickstart
If you want to learn how it works, for now please refer to the [Wiki](https://github.com/couchbaselabs/couchbase-spark-connector/wiki). It
currently acts as the primary resource to get started.

## License
Copyright 2015 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See [the Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).