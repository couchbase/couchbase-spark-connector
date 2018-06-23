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
Spark RDD functions
"""
import pyspark
from pyspark.rdd import RDD
from pyspark_couchbase.utils import (
    _scala_api, _get_java_array)


def monkey_patch_rdd():
    """
    Add methods to RDD class
    """
    pyspark.rdd.RDD.couchbaseGet = couchbaseGet
    pyspark.rdd.RDD.saveToCouchbase = saveToCouchbase
    pyspark.rdd.RDD.couchbaseSubdocLookup = couchbaseSubdocLookup


def _get_python_rdd(ctx, rdd_scala):
    """
    Convenience method to serialize scala RDD and
    convert to python RDD
    """
    rdd_pickled = _scala_api(
        ctx).pickleRows(rdd_scala)
    jrdd = _scala_api(
        ctx).javaRDD(rdd_pickled)
    return RDD(jrdd, ctx)


def couchbaseGet(
        rdd, bucketName=None, timeout=0,
        timeoutUnit="millis"):
    """
    Query couchbase with an RDD of document ids.

    Calls RDD[String].couchbaseGet[RawJsonDocument] on scala

    Parameters
    ----------
    rdd: RDD
        rdd of strings corresponding to document ids
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
        Returns RDD of RawJsonDocument for found documents

    """
    # Convert python RDD to JavaRDD. No need to deserialize
    # as it's RDD[String]
    rdd_java = rdd.ctx._jvm.SerDe.pythonToJava(rdd._jrdd, True)

    # call RDD[String].couchbaseGet
    rdd_scala = _scala_api(
        rdd.ctx).couchbaseGet(
        rdd_java.rdd(), bucketName, timeout, timeoutUnit)
    return _get_python_rdd(rdd.ctx, rdd_scala)


def saveToCouchbase(
        rdd, bucketName=None, storeMode="UPSERT",
        timeout=0, timeoutUnit="millis"):
    """
    Save RDD of RawJsonDocuments to couchbase.

    Calls RDD[RawJsonDocument].saveToCouchbase

    Parameters
    ----------
    rdd: RDD
        rdd of strings corresponding to document ids
    bucketName: string
        Bucket to connect. If None,
        it connects to the only configured bucket
    timeout: int
        timeout amount
    storeMode: string
        one of "UPSERT", "INSERT_AND_FAIL", "INSERT_AND_IGNORE",
        "REPLACE_AND_FAIL", "REPLACE_AND_IGNORE" corresponding
        to store modes.
    timeoutUnit: string
        unit of timeout in scala.concurrent.duration.Duration
        specified as string. Available options are;
        "days", "hours", "micros", "millis", "minutes",
        "nanos", "seconds".
    """
    unpickled = _scala_api(
        rdd.ctx).unpickleRows(rdd._jrdd.rdd())
    _scala_api(
        rdd.ctx).saveToCouchbase(
        unpickled, bucketName, storeMode, timeout, timeoutUnit)


def couchbaseSubdocLookup(rdd, get, exists=None, bucketName=None,
                          timeout=0, timeoutUnit="millis"):
    """
    Create a subdoclookup query from RDD of document
    ids.

    Calls RDD[String].couchbaseSubdocLookup on scala

    Parameters
    ----------
    rdd: RDD
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
    get_java = _get_java_array(rdd.ctx._gateway, "String", get)
    exists_java = _get_java_array(rdd.ctx._gateway, "String", exists)
    rdd_java = rdd.ctx._jvm.SerDe.pythonToJava(rdd._jrdd, True)
    rdd_scala = _scala_api(
        rdd.ctx).couchbaseSubdocLookup(
        rdd_java.rdd(), get_java, exists_java, bucketName,
        timeout, timeoutUnit)
    return _get_python_rdd(rdd.ctx, rdd_scala)
