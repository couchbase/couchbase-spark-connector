# Couchbase Spark Connector

Welcome to the official couchbase spark connector! It provides functionality to chain together both Couchbase Server and Spark
in various versions and combinations. It is still under development, so use with care and open issues if you come across them.

If you want to learn how it works, for now please refer to the [Wiki](https://github.com/couchbaselabs/couchbase-spark-connector/wiki). It
currently acts as the primary resource to get started.

## Todo

General:

- migrate the whole thing to sbt (build to 2.10 and 2.11, generate scaladoc,...)
- add docs on how to set up stub projects
- add docs on some simple samples to run with the `beer-sample` bucket

Features:

- Support Spark Streaming through DCP
- Support Spark SQL through tight N1QL integration
- Support Java RDDs on all stuff

Enhancements:

- Make sure connections are properly closed when not needed anymore
- Support callbacks on connection ("with bucket", "with cluster",...)
