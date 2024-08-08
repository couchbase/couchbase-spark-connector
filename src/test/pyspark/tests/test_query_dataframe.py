from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
load_dotenv()

spark = SparkSession.builder \
    .config("spark.jars", os.getenv('COUCHBASE_SPARK_JAR')) \
    .config("spark.couchbase.connectionString", os.getenv('COUCHBASE_CONNECTION_STRING')) \
    .config("spark.couchbase.username", os.getenv('COUCHBASE_USERNAME')) \
    .config("spark.couchbase.password", os.getenv('COUCHBASE_PASSWORD')) \
    .config("spark.ssl.insecure", os.getenv('COUCHBASE_TLS_INSECURE')) \
    .getOrCreate()

def test_basic():
    df = (spark.read.format("couchbase.query")
          .option("bucket", "travel-sample")
          .option("scope", "inventory")
          .option("collection", "airline")
          .load())

    df.printSchema()
    df.show()

    assert df.count() == 187

def test_custom_schema():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("callsign", StringType(), True),
        StructField("country", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("justToEnsureThisSchemaIsUsed", StringType(), True)
    ])
    df = (spark.read.format("couchbase.query")
          .schema(schema)
          .option("bucket", "travel-sample")
          .option("scope", "inventory")
          .option("collection", "airline")
          .load())

    print(schema)
    df.printSchema()
    assert schema == df.schema

    assert df.count() == 187
    for x in df.collect():
        assert x['justToEnsureThisSchemaIsUsed'] == None


if __name__ == "__main__":
    test_basic()
