import logging
import uuid

from pyspark.sql import SparkSession
from dotenv import load_dotenv
import pytest
import os
from src.test.pyspark.tests.util_and_resources import util_and_resources

load_dotenv()

logger = logging.getLogger(__name__)

WRONG_CONNECTION_STRING = "wrong_connection_string"
WRONG_USERNAME = "wrong_username"
WRONG_PASSWORD = "wrong_password"
WRONG_SSL_TYPE = "wrong_ssl_type"
CURR_SETUP = util_and_resources.get_curr_cluster_setup()

pytestmark = pytest.mark.parametrize("curr_connection_id",
                                     [util_and_resources.PRIMARY_CONNECTION_ID, util_and_resources.TEST_CONNECTION_ID])


def fetch_dataframe_and_assert_count(spark, curr_connection_id=util_and_resources.PRIMARY_CONNECTION_ID):
    options = {
        CURR_SETUP.bucket_or_db: util_and_resources.TRAVEL_SAMPLE_BUCKET,
        "scope": util_and_resources.INVENTORY_SCOPE,
        "collection": util_and_resources.AIRLINE_COLLECTION,
    }

    if curr_connection_id != util_and_resources.PRIMARY_CONNECTION_ID:
        options["connectionIdentifier"] = curr_connection_id

    df = (spark.read.format(CURR_SETUP.df_type)
          .options(**options)
          .load()).limit(5)

    df.printSchema()
    df.show()
    assert df.count() == 5

    return df


# Due to SPARKC-217, the following tests can't be run together
# as they will fail due to the same SparkSession being used
class TestPysparkSession:
    spark_session = None

    @pytest.fixture(scope="class", autouse=True)
    def class_fixture(self, request, shared_spark_session):
        request.cls.spark_session = shared_spark_session

    def test_missing_connection_string(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "IllegalArgumentException" in str(e.type)
        assert str(
            e.value) == f'Required config property spark.couchbase.connectionString{connection_identifier} is not present'

    def test_missing_username(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "IllegalArgumentException" in str(e.type)
        assert str(
            e.value) == "when one of spark.couchbase.username or spark.couchbase.password is specified, then both must be specified"

    def test_missing_password(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "IllegalArgumentException" in str(e.type)
        assert str(
            e.value) == "when one of spark.couchbase.username or spark.couchbase.password is specified, then both must be specified"

    def test_connection_string_as_none(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", None)
                     .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "IllegalArgumentException" in str(e.type)
        assert str(e.value) == f'The value of property spark.couchbase.connectionString must not be null'

    def test_username_as_none(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, None)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "IllegalArgumentException" in str(e.type)
        assert str(
            e.value) == f'The value of property spark.couchbase.username{connection_identifier} must not be null'

    def test_password_as_none(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, None)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "IllegalArgumentException" in str(e.type)
        assert str(
            e.value) == f'The value of property spark.couchbase.password{connection_identifier} must not be null'

    def test_wrong_cred(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", WRONG_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, WRONG_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, WRONG_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, WRONG_SSL_TYPE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "IllegalArgumentException" in str(e.type)
        assert str(e.value) == f'For input string: "{WRONG_SSL_TYPE}"'

    # Long-running test (30s) - Asserting AmbiguousTimeoutException
    def test_wrong_connection_string(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", WRONG_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "AmbiguousTimeoutException" in str(e.value)

    # Long-running test (30s) - Asserting AmbiguousTimeoutException
    def test_wrong_username(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, WRONG_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "AmbiguousTimeoutException" in str(e.value)

    # Long-running test (30s) - Asserting AmbiguousTimeoutException
    def test_wrong_password(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, WRONG_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "AmbiguousTimeoutException" in str(e.value)

    def test_wrong_ssl_type(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, WRONG_SSL_TYPE)
                     .getOrCreate())

            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert str(e.value) == "For input string: \"wrong_ssl_type\""

    def test_happy_path(self, curr_connection_id, shared_sdk_connection):
        connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)

        unique_id = str(uuid.uuid4())
        write_test_collection = {
            "bucket": util_and_resources.TRAVEL_SAMPLE_BUCKET,
            "scope": "test_" + unique_id,
            "collection": "test_" + unique_id,
            "idFieldName": "id",
        }

        spark = (SparkSession.builder
                 .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                 .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                 .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                 .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                 .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                 .getOrCreate())

        df = fetch_dataframe_and_assert_count(spark, curr_connection_id).limit(5)

        assert df.count() == 5

        # write.format not supported in Columnar dataframes
        if CURR_SETUP.df_type is not util_and_resources.CB_COLUMNAR:
            id_list = df.select("id").rdd.flatMap(lambda x: x).collect()

            try:
                shared_sdk_connection.create_scope(write_test_collection["scope"])
                shared_sdk_connection.create_collection(write_test_collection["collection"],
                                                        write_test_collection["scope"])
                shared_sdk_connection.create_primary_index(write_test_collection["collection"],
                                                           write_test_collection["scope"])

                (df.write.format(util_and_resources.CB_QUERY)
                 .options(**write_test_collection)
                 .save())

                read_df = (spark.read.format(util_and_resources.CB_QUERY)
                           .options(**write_test_collection)
                           .load())

                updated_df_id_list = read_df.select("id").rdd.flatMap(lambda x: x).collect()
                updated_df_id_list = sorted(list(map(int, updated_df_id_list)))

                assert read_df.count() == 5
                assert updated_df_id_list == id_list

            except Exception as e:
                raise e
            finally:
                shared_sdk_connection.drop_scope(write_test_collection["scope"])

        spark.stop()

    def test_two_consecutive_connections(self, curr_connection_id):
        with pytest.raises(Exception) as e:
            connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            logger.info("Dataset by first connection: ")
            fetch_dataframe_and_assert_count(spark, curr_connection_id)
            spark.stop()

            spark = (SparkSession.builder
                     .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                     .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                     .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                     .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                     .getOrCreate())

            logger.info("Second connection should raise IllegalArgumentException as username is missing.")
            fetch_dataframe_and_assert_count(spark, curr_connection_id)

        spark.stop()
        assert "IllegalArgumentException" in str(e.type)
        assert str(
            e.value) == "when one of spark.couchbase.username or spark.couchbase.password is specified, then both must be specified"

    @pytest.mark.skip(reason="Initial setup for this test needs ssh to capella that will be tricky to automate.")
    def test_client_cert(self, curr_connection_id):
        spark = (SparkSession.builder
                 .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                 .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                 .config("spark.couchbase.keyStorePath", os.getenv('KEY_STORE_PATH'))
                 .config("spark.couchbase.keyStorePassword", os.getenv('KEY_STORE_PASSWORD'))
                 .config("spark.couchbase.keyStoreType", os.getenv('KEY_STORE_TYPE'))
                 .config("spark.couchbase.security.trustCertificate", os.getenv('COUCHBASE_TRUST_CERT'))
                 .config("spark.couchbase.security.enableTls", not util_and_resources.COUCHBASE_SSL_INSECURE)
                 .getOrCreate())

        fetch_dataframe_and_assert_count(spark)
        spark.stop()

    @pytest.mark.skipif(CURR_SETUP.df_type == util_and_resources.CB_COLUMNAR,
                        reason="Implicit connection not supported for Columnar dataframes")
    def test_implicit_connection(self, curr_connection_id):
        connection_identifier = util_and_resources.get_connection_identifier(curr_connection_id)
        spark = (SparkSession.builder
                 .config("spark.jars", util_and_resources.COUCHBASE_SPARK_JAR)
                 .config("spark.couchbase.connectionString", util_and_resources.COUCHBASE_CONNECTION_STRING)
                 .config("spark.couchbase.username" + connection_identifier, util_and_resources.COUCHBASE_USERNAME)
                 .config("spark.couchbase.password" + connection_identifier, util_and_resources.COUCHBASE_PASSWORD)
                 .config("spark.ssl.insecure" + connection_identifier, util_and_resources.COUCHBASE_SSL_INSECURE)
                 .config("spark.couchbase.implicitBucket", util_and_resources.TRAVEL_SAMPLE_BUCKET)
                 .config("spark.couchbase.implicitScope", util_and_resources.INVENTORY_SCOPE)
                 .config("spark.couchbase.implicitCollection", util_and_resources.AIRLINE_COLLECTION)
                 .getOrCreate())

        options = {}
        if curr_connection_id != util_and_resources.PRIMARY_CONNECTION_ID:
            options["connectionIdentifier"] = curr_connection_id

        df = (spark.read.format(CURR_SETUP.df_type)
              .options(**options)
              .load()).limit(5)

        df.printSchema()
        df.show()

        assert df.count() == 5

        spark.stop()
