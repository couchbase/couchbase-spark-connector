import unittest
import json
import glob
import os

from pyspark.sql import SparkSession
import pyspark_couchbase
from pyspark_couchbase.types import (
    RawJsonDocument)


BUCKETNAME = os.environ.get("BUCKETNAME", "default")
BUCKETPASSWORD = os.environ.get("BUCKETPASSWORD", "password")
JARS = glob.glob(
    os.path.join(os.environ.get("JARS_PATH", ""), "*.jar"))


class TestLocal(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder\
            .master("local")\
            .appName("test-integration")\
            .config("spark.jars", ",".join(JARS))\
            .config("spark.couchbase.nodes", "127.0.0.1")\
            .config("spark.couchbase.bucket.{}".format(
                BUCKETNAME), BUCKETPASSWORD)\
            .getOrCreate()

    def test_rawjson_save_get_from_context(self):
        self.spark.sparkContext.parallelize(
            [RawJsonDocument("doc_10", json.dumps({"val": "doc_10"})),
             RawJsonDocument("doc_20", json.dumps({"val": "doc_20"})),
             RawJsonDocument("doc_30", json.dumps({"val": "doc_30"}))])\
            .saveToCouchbase()

        for doc in self.spark.sparkContext.couchbaseGet(
                ["doc_10", "doc_20", "doc_30"]).collect():
            assert doc.id == json.loads(doc.content)["val"]

    def test_rawjson_save_get_from_rdd(self):
        self.spark.sparkContext.parallelize(
            [RawJsonDocument("doc_11", json.dumps({"val": "doc_11"})),
             RawJsonDocument("doc_21", json.dumps({"val": "doc_21"})),
             RawJsonDocument("doc_31", json.dumps({"val": "doc_31"}))])\
            .saveToCouchbase()

        for doc in self.spark.sparkContext.parallelize(
               ["doc_11", "doc_21", "doc_31"]).couchbaseGet().collect():
            assert doc.id == json.loads(doc.content)["val"]

    def test_subdoclookup_from_context(self):
        self.spark.sparkContext.parallelize(
            [RawJsonDocument(
                "subdoc_10", json.dumps({"subdoc": {"val": "subdoc_10"}})),
             RawJsonDocument(
                "subdoc_20", json.dumps({"subdoc": {"val": "subdoc_20"}})),
             RawJsonDocument(
                "subdoc_30", json.dumps({"subdoc": {"val": "subdoc_30"}}))])\
            .saveToCouchbase()

        for doc in self.spark.sparkContext.couchbaseSubdocLookup(
                ["subdoc_10", "subdoc_20", "subdoc_30"],
                ["subdoc"]).collect():
            assert doc.id == doc.content["subdoc"]["val"]

    def test_subdoclookup_from_rdd(self):
        self.spark.sparkContext.parallelize(
            [RawJsonDocument(
                "subdoc_10", json.dumps({"subdoc": {"val": "subdoc_10"}})),
             RawJsonDocument(
                "subdoc_20", json.dumps({"subdoc": {"val": "subdoc_20"}})),
             RawJsonDocument(
                "subdoc_30", json.dumps({"subdoc": {"val": "subdoc_30"}}))])\
            .saveToCouchbase()

        for doc in self.spark.sparkContext.parallelize(
                ["subdoc_10", "subdoc_20", "subdoc_30"]).couchbaseSubdocLookup(
                ["subdoc"]).collect():
            assert doc.id == doc.content["subdoc"]["val"]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
