import pytest
import logging
from utils import util_and_resources
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("class_fixture")
class TestSchema:
    spark_session = None
    curr_setup = util_and_resources.get_curr_cluster_setup()

    @pytest.fixture(scope="class", autouse=True)
    def class_fixture(self, request, shared_spark_session):
        request.cls.spark_session = shared_spark_session

    def test_custom_schema_explicit(self):
        id_type = IntegerType() if self.curr_setup.df_type == util_and_resources.CB_QUERY else StringType()
        schema = StructType([
            StructField("callsign", StringType(), True),
            StructField("country", StringType(), True),
            StructField("iata", StringType(), True),
            StructField("icao", StringType(), True),
            StructField("id", id_type, True),
            StructField("name", StringType(), True),
            StructField("justToEnsureThisSchemaIsUsed", StringType(), True)
        ])

        df = (self.spark_session.read.format(self.curr_setup.df_type)
              .schema(schema)
              .option(self.curr_setup.bucket_or_db, util_and_resources.TRAVEL_SAMPLE_BUCKET)
              .option("scope", util_and_resources.INVENTORY_SCOPE)
              .option("collection", util_and_resources.AIRLINE_COLLECTION)
              .load())

        logger.info("Expected Schema:\n%s", schema.simpleString())
        df.printSchema()
        assert schema == df.schema

        assert df.count() == 187
        for x in df.collect():
            assert x['justToEnsureThisSchemaIsUsed'] is None
