from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
load_dotenv()

# Initialise Spark
spark = SparkSession.builder \
    .config("spark.couchbase.connectionString", os.getenv('COUCHBASE_CONNECTION_STRING')) \
    .config("spark.couchbase.username", os.getenv('COUCHBASE_USERNAME')) \
    .config("spark.couchbase.password", os.getenv('COUCHBASE_PASSWORD')) \
    .getOrCreate()

# Load a DataFrame from Couchbase
df = (spark.read.format("couchbase.query")
      .option("bucket", "travel-sample")
      .option("scope", "inventory")
      .option("collection", "airline")
      .load())
df.printSchema()
df.show()
spark.stop()
