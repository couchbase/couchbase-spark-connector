# Couchbase Spark Connector

Welcome to the official couchbase spark connector! It provides functionality to chain together both Couchbase Server and Spark
in various versions and combinations. It is still under development, so use with care and open issues if you come across them.

If you want to learn how it works, for now please refer to the [Wiki](https://github.com/couchbaselabs/couchbase-spark-connector/wiki). It
currently acts as the primary resource to get started.

## Quickstart

A sample `build.sbt`:

```scala
name := "your-couchbase-spark-app"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.couchbase.client" %% "spark-connector" % "1.0.0-dp",
  "org.apache.spark" %% "spark-streaming" % "1.2.1"
)

resolvers += "Couchbase Repository" at "http://files.couchbase.com/maven2/"
```

The wiki docs provide examples how to read and write to couchbase - have fun!

## License

Copyright 2015 Couchbase Inc.

Licensed under the Apache License, Version 2.0.

See [the Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).