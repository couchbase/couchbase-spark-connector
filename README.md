# Couchbase Spark Connector

.. currently rewriting this, see the wiki for more info as well ..

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
