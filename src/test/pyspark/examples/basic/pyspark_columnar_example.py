from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

# Initialise Spark
spark = SparkSession.builder \
    .appName("Couchbase Spark Connector Columnar Example") \
    .config("spark.couchbase.connectionString", os.getenv('COUCHBASE_CONNECTION_STRING')) \
    .config("spark.couchbase.username", os.getenv('COUCHBASE_USERNAME')) \
    .config("spark.couchbase.password", os.getenv('COUCHBASE_PASSWORD')) \
    .config("spark.ssl.insecure", "true") \
    .getOrCreate()

# Load a DataFrame from Couchbase columnar
df = (spark.read.format("couchbase.columnar")
      .option("database", "travel-sample")
      .option("scope", "inventory")
      .option("collection", "airline")
      .load())
df.printSchema()
df.show()



spark.stop()
