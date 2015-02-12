# Couchbase Spark Connector

This stuff is a work in progress, but you can already load documents out of Couchbase into an RDD. Be prepared for
world domination soon!

## Connecting
By default the connector will connect to `localhost` on the `default` bucket:

```scala
// Use the local master for testing, no distributed setup
val conf = new SparkConf().setMaster("local").setAppName("example")
val sc = new SparkContext(conf)
```

If you want to tune that, you can change the spark config:

```scala
val conf = new SparkConf()
  .setMaster("local")
  .setAppName("test")
  // You can customize this for now:
  .set("couchbase.host", "192.168.56.101")
  .set("couchbase.bucket", "mybucket")
  .set("couchbase.password", "password")
```

## Usage
That's it, you can now fetch documents from the bucket. You want to `import com.couchbase.spark._` so you get all
the nice couchbase methods on your spark context.

Here is how to get a `RDD` which contains `JsonDocument`s:

```scala
// RDD will contain all found docs
val docs = sc.couchbaseGet[JsonDocument](Seq("doc1", "doc2", "doc3"))

// You can also customize the response document type from the SDK
val doc = sc.couchbaseGet[RawJsonDocument]("rawDoc")
```


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

## Todo

- Figure out how to make use of couchbase rdds in map functions and therelike without serialization issues
- Support writing RDDs to Couchbase
- Support "document" method on ViewRDD which will return a new DocumentRDD with all the row id contents
- Support QueryRDD (N1QL)
- Support connecting to multiple clusters/buckets in an easy fashion
- Support Partitions on DocumentRDD (to split up bulk gets over multiple workers)
- Support Spark Streaming through DCP
- Support Spark SQL through tight N1QL integration