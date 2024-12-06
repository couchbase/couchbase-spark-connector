import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession
from dotenv import load_dotenv

import os

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

assert (COUCHBASE_CONNECTION_STRING := os.getenv("COUCHBASE_CONNECTION_STRING")), "COUCHBASE_CONNECTION_STRING not set"
assert (CLUSTER_TYPE := os.getenv('CLUSTER_TYPE')), "CLUSTER_TYPE not set"
assert (COUCHBASE_USERNAME := os.getenv("COUCHBASE_USERNAME")), "COUCHBASE_USERNAME not set"
assert (COUCHBASE_PASSWORD := os.getenv("COUCHBASE_PASSWORD")), "COUCHBASE_PASSWORD not set"
assert (COUCHBASE_SPARK_JAR := os.getenv("COUCHBASE_SPARK_JAR")), "COUCHBASE_SPARK_JAR not set"
COUCHBASE_SSL_INSECURE = os.getenv("COUCHBASE_SSL_INSECURE", "false").lower()

CB_KV = "couchbase.kv"
CB_QUERY = "couchbase.query"
CB_COLUMNAR = "couchbase.columnar"
TRAVEL_SAMPLE_BUCKET = "travel-sample"
INVENTORY_SCOPE = "inventory"
AIRLINE_COLLECTION = "airline"
TEST_CONNECTION_ID = "testConnectionId"
PRIMARY_CONNECTION_ID = "primaryConnectionId"


@dataclass
class CurrentClusterSetup:
    df_type: str
    bucket_or_db: str


def get_connection_identifier(curr_connection_id):
    if curr_connection_id == PRIMARY_CONNECTION_ID:
        return ''
    return f':{curr_connection_id}'


def get_curr_cluster_setup() -> CurrentClusterSetup:
    if CLUSTER_TYPE.strip().lower() == 'columnar':
        return CurrentClusterSetup(df_type=CB_COLUMNAR, bucket_or_db="database")
    elif CLUSTER_TYPE.strip().lower() == 'operational':
        return CurrentClusterSetup(df_type=CB_QUERY, bucket_or_db="bucket")
    else:
        raise ValueError(
            "Invalid cluster type. Please set CLUSTER_TYPE to either 'columnar' or 'operational' in .env file.")


def create_spark_session(curr_connection_id=PRIMARY_CONNECTION_ID):
    try:
        return (SparkSession.builder
                .config("spark.jars", COUCHBASE_SPARK_JAR)
                .config("spark.couchbase.connectionString", COUCHBASE_CONNECTION_STRING)
                .config("spark.couchbase.username" + get_connection_identifier(curr_connection_id), COUCHBASE_USERNAME)
                .config("spark.couchbase.password" + get_connection_identifier(curr_connection_id), COUCHBASE_PASSWORD)
                .config("spark.ssl.insecure" + get_connection_identifier(curr_connection_id), COUCHBASE_SSL_INSECURE)
                ).getOrCreate()

    except Exception as e:
        logger.info("Error establishing spark connection.")
        raise e


def get_explicit_df(spark_session, curr_connection_id=PRIMARY_CONNECTION_ID, **options):
    if curr_connection_id != PRIMARY_CONNECTION_ID:
        options["connectionIdentifier"] = curr_connection_id

    curr_cluster_setup = get_curr_cluster_setup()
    return (spark_session.read.format(curr_cluster_setup.df_type)
            .option(curr_cluster_setup.bucket_or_db, TRAVEL_SAMPLE_BUCKET)
            .option("scope", INVENTORY_SCOPE)
            .option("collection", AIRLINE_COLLECTION)
            .options(**options)
            .load())
