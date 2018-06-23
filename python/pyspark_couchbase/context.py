#
# Copyright (c) 2015 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Spark context functions
"""
import pyspark
from pyspark.rdd import RDD

from pyspark_couchbase.utils import (
    _get_java_array, _scala_api)


def monkey_patch_context():
    """
    Add methods to SparkContext class
    """
    pyspark.context.SparkContext.couchbaseGet = couchbaseGet
    pyspark.context.SparkContext.couchbaseSubdocLookup = \
        couchbaseSubdocLookup


def couchbaseGet(sc, ids, bucketName=None, numSlices=0,
                 timeout=0, timeoutUnit="millis"):
    """
    Query couchbase with a list of document ids,
    returns RDD of RawJsonDocuments.

    Calls sparkContext.couchbaseGet[RawJsonDocument] on scala

    Parameters
    ----------
    sc: sparkContext
    ids: List of strings
        List of strings corresponding to document ids
    bucketName: string
        Bucket to connect. If None,
        it connects to the only configured bucket
    numSlices: int
        parallelism, defaults to spark defaultParallelism
        if 0
    timeout: int
        timeout amount
    timeoutUnit: string
        unit of timeout in scala.concurrent.duration.Duration
        specified as string. Available options are;
        "days", "hours", "micros", "millis", "minutes",
        "nanos", "seconds".

    Returns
    -------
    RDD
        Returns RDD of RawJsonDocument for found documents
    """
    # Convert list of strings to java as Array[String]
    java_ids = _get_java_array(sc._gateway, "String", ids)

    # Call sparkContext.couchbaseGet.
    rdd_results = _scala_api(
        sc).couchbaseGet(
        sc._jsc.sc(), java_ids, bucketName, numSlices, timeout, timeoutUnit)

    # Serialize scala RDD
    rdd_pickled = _scala_api(
        sc).pickleRows(rdd_results)

    # Convert RDD to JavaRDD and create python RDD
    jrdd = _scala_api(
        sc).javaRDD(rdd_pickled)
    return RDD(jrdd, sc)


def couchbaseSubdocLookup(sc, ids, get,
                          exists=None, bucketName=None,
                          timeout=0, timeoutUnit="millis"):
    """
    Create a subdoclookup query from list of document
    ids.

    Calls sparkContext.couchbaseSubdocLookup on scala

    Parameters
    ----------
    sc: sparkContext
    ids: List of strings
        rdd of strings corresponding to document ids
    get: List of strings
        fields to fetch from document
    exists: List of strings
        optionally check if keys exist in the document
    bucketName: string
        Bucket to connect. If None,
        it connects to the only configured bucket
    timeout: int
        timeout amount
    timeoutUnit: string
        unit of timeout in scala.concurrent.duration.Duration
        specified as string. Available options are;
        "days", "hours", "micros", "millis", "minutes",
        "nanos", "seconds".

    Returns
    -------
    RDD
        Returns RDD of SubdocLookupResult for found documents
    """
    if exists is None:
        exists = []
    # Convert list of strings to java as Array[String]
    ids_java = _get_java_array(sc._gateway, "String", ids)
    get_java = _get_java_array(sc._gateway, "String", get)
    exists_java = _get_java_array(sc._gateway, "String", exists)

    # Call sparkContext.couchbaseSubdocLookup
    rdd_results = _scala_api(
        sc).couchbaseSubdocLookup(
        sc._jsc.sc(), ids_java, get_java, exists_java, bucketName,
        timeout, timeoutUnit)

    # Serialize scala RDD
    rdd_pickled = _scala_api(
        sc).pickleRows(rdd_results)

    # Convert RDD to JavaRDD and create python RDD
    jrdd = _scala_api(
        sc).javaRDD(rdd_pickled)
    return RDD(jrdd, sc)
