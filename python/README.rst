=================
pyspark_couchbase
=================

This package provides support for working with pyspark RDDs using 
couchbase server as data source.

--------
Building
--------

Building assembly jar for couchbase spark connector.

.. code-block:: sh

    sbt assembly

Building an egg file to submit it with spark-submit.

.. code-block:: sh

    cd python && python setup.py bdist_egg


Installing to current environment for interactive use.

.. code-block:: sh

    cd python && python setup.py install

----------------
Using with spark
----------------

After building an assembly jar and an egg file;

.. code-block:: sh

    spark-submit \
       --jars path/to/spark-connector-assembly<version>.jar \
       --py-files path/to/pyspark_couchbase<version>.egg

Using spark packages instead of assembly jar;

.. code-block:: sh

    spark-submit \
       --packages com.couchbase.client:spark-connector:<version> \
       --py-files path/to/pyspark_couchbase<version>.egg

In order to make pyspark_couchbase functions available in
sparkContext and RDD's, import pypspark_couchbase in the shell or
submitted python script. 

.. code-block:: python

    import pyspark_couchbase

--------
Examples
--------

Creating a spark session, connecting to a local couchbase server and
setting configuration for a bucket and its password;

.. code-block:: python

    import pyspark_couchbase
    from pyspark.sql import SparkSession

    spark = SparkSession.builder\
       .master("local")\
       .appName("test")\
       .config("spark.couchbase.nodes", "127.0.0.1")\
       .config("spark.couchbase.bucket.bucketname", "bucketpassword")\
       .getOrCreate()

Saving RDD of RawJsonDocuments couchbase

.. code-block:: python

    from pyspark_couchbase.types import RawJsonDocument

    spark.sparkContext.parallelize(
        [RawJsonDocument("doc_1", json.dumps({"val": "doc_1"}))])\
        .saveToCouchbase()


Getting data from couchbase using spark context or RDD of document ids;

.. code-block:: python

    docs = spark.sparkContext.couhcbaseGet(["doc_1"]).collect()


.. code-block:: python

    docs = spark.sparkContext.parallelize(["doc_1"]).couchbaseGet().collect()

.. code-block:: python

    >> docs[0].id
    u'doc_1'
    >> docs[0].content
    u'{"val": "doc_1"}'


Subdoclookup queries;

.. code-block:: python

    from pyspark_couchbase.types import RawJsonDocument
    import json
    spark.sparkContext.parallelize(
        [RawJsonDocument(
             "subdoc_10", json.dumps({"subdoc": {"val": "subdoc_10"}})])])\
        .saveToCouchbase()

.. code-block:: python

    results = spark.sparkContext.parallelize(["subdoc_10"]).couchbaseSubdocLookup(
        ["subdoc"]).collect()

.. code-block:: python 

    results = spark.sparkContext.couchbaseSubdocLookup(
        ["subdoc_10"], ["subdoc"]).collect()

.. code-block:: python

    >> results[0].id
    u'subdoc_10'
    >> results[0].content
    {u'subdoc': {u'val': u'subdoc_10'}}
