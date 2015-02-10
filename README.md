# Couchbase Spark Connector

This stuff is a work in progress, but you can already load documents out of Couchbase into an RDD. Be prepared for
world domination soon!

## Building the Connector
Since you need to have all of this on your classpath distributed to the worker nodes, there are essentially two
ways to make it work:

 1) You build your app with this dependency and create a "fat jar". Then you submit the job and all is well.
 2) You put only put the dependency jar on the classpath of all your nodes and build your application jar without
    the dependency.

I think number 1 is much simpler, but you need to know some build tool foo to pull it off. Thankfully, gradle makes
it very easy. You can use the `shadow` plugin to just grab all your stuff and then you can call the `shadowJar` task
to build the uber jar. You can then deploy it, but don't forget to point it to the proper main class on submitting.

There will be better instructions soon.