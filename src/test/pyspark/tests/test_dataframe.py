import logging
import uuid
import pytest

from src.test.pyspark.tests.util_and_resources import util_and_resources

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("class_fixture")
class TestDataframe:
    spark_session = None
    curr_setup = util_and_resources.get_curr_cluster_setup()

    @pytest.fixture(scope="class", autouse=True)
    def class_fixture(self, request, shared_spark_session):
        request.cls.spark_session = shared_spark_session

    def test_basic(self):
        df = util_and_resources.get_explicit_df(self.spark_session)
        df.printSchema()
        df.show()

        assert "id" in df.columns
        assert df.count() == 187

    def test_query(self):
        df = util_and_resources.get_explicit_df(self.spark_session)

        query_result = (df.select("name", "callsign", "country")
                        .where("callsign != 'NULL'")
                        .limit(10)
                        .orderBy("country")
                        .sort("callsign"))

        query_result.printSchema()
        query_result.show()

        assert query_result.count() == 10

        callsign_list = []

        for row in query_result.collect():
            callsign_list.append(row['callsign'])

        assert len(callsign_list) == 10
        assert callsign_list == sorted(callsign_list)

    def test_create_tmp_view(self):
        df = util_and_resources.get_explicit_df(self.spark_session)

        df.createOrReplaceTempView("airlinesView")

        query_result = self.spark_session.sql(
            "SELECT name, callsign, country "
            "FROM airlinesView "
            "WHERE callsign != 'NULL' "
            "SORT BY callsign "
            "LIMIT 10;")

        query_result.printSchema()
        query_result.show()

        assert len(query_result.columns) == 3
        assert "name" in query_result.columns
        assert "callsign" in query_result.columns
        assert "country" in query_result.columns
        assert query_result.count() == 10

        callsign_list = []

        for row in query_result.collect():
            callsign_list.append(row['callsign'])

        assert len(callsign_list) == 10
        assert callsign_list == sorted(callsign_list)

    def test_filter_option(self):
        test_options = {
            "filter": "country = 'France'",
        }

        df = util_and_resources.get_explicit_df(self.spark_session, **test_options)
        df.printSchema()
        df.show()

        assert df.count() == 21

        for row in df.collect():
            assert row['country'] == "France"

    @pytest.mark.skipif(curr_setup.df_type != util_and_resources.CB_COLUMNAR,
                        reason="this test needs completed_requests to have {\"completed-threshold\":0} so that it will "
                               "catch all requests. And capella operational don't support adjusting this setting.")
    def test_scan_consistency_option(self, shared_sdk_connection):
        unique_id = str(uuid.uuid4())

        shared_sdk_connection.insert_doc(f'{unique_id}1', {"test_name": unique_id},
                                         util_and_resources.AIRLINE_COLLECTION, util_and_resources.INVENTORY_SCOPE)
        shared_sdk_connection.insert_doc(f'{unique_id}2', {"test_name": unique_id},
                                         util_and_resources.AIRLINE_COLLECTION, util_and_resources.INVENTORY_SCOPE)

        test_options = {
            "filter": f'test_name = "{unique_id}"',
            "scanConsistency": "requestPlus",
        }

        df = util_and_resources.get_explicit_df(self.spark_session, **test_options)
        assert df.count() == 2

        shared_sdk_connection.delete_doc(f'{unique_id}1', util_and_resources.AIRLINE_COLLECTION)
        shared_sdk_connection.delete_doc(f'{unique_id}2', util_and_resources.AIRLINE_COLLECTION)

        list_of_completed_requests = (shared_sdk_connection.get_list_of_completed_requests())

        if self.curr_setup.df_type == util_and_resources.CB_COLUMNAR:
            completed_requests_from_test = filter(
                lambda req: "select" in req.get("statement").lower() and unique_id in req["statement"],
                list_of_completed_requests.get_all_rows()[0])
        else:
            completed_requests_from_test = filter(lambda req: "select" in req.get("completed_requests").get(
                "statement").lower() and f'test_name = "{unique_id}"' in req.get("completed_requests").get(
                "statement"), list_of_completed_requests)

            completed_requests_from_test = [req.get("completed_requests") for req in completed_requests_from_test]

        for curr_req in completed_requests_from_test:
            assert curr_req["scanConsistency"] == (
                'request_plus' if self.curr_setup.df_type == util_and_resources.CB_COLUMNAR else "scan_plus")

            logger.info(
                f'Current Request: scanConsistency - {curr_req["scanConsistency"]} and statement - {curr_req["statement"]}')

    def test_infer_limit_option(self, shared_sdk_connection):
        unique_id = str(uuid.uuid4())
        test_options = {
            self.curr_setup.bucket_or_db: util_and_resources.TRAVEL_SAMPLE_BUCKET,
            "scope": "test_" + unique_id,
            "collection": "test_" + unique_id,
            "inferLimit": "1",
            "idFieldName": "id",
        }

        try:
            # Creating new scope & collection as new docs need to be added to collection
            # and this will change doc count for other tests
            shared_sdk_connection.create_scope(test_options["scope"])
            shared_sdk_connection.create_collection(test_options["collection"], test_options["scope"])
            shared_sdk_connection.create_primary_index(test_options["collection"], test_options["scope"])
            shared_sdk_connection.insert_doc("1", {"name": "test"}, test_options["collection"], test_options["scope"])
            shared_sdk_connection.insert_doc("2", {"foo": "bar"}, test_options["collection"], test_options["scope"])

            df = util_and_resources.get_explicit_df(self.spark_session, **test_options)

            assert len(df.columns) == 2

            assert (("foo" in df.columns or "name" in df.columns) and not
            ("foo" in df.columns and "name" in df.columns))

            assert df.count() == 2

        finally:
            shared_sdk_connection.drop_scope(test_options["scope"])

    def test_timeout_option(self):
        with pytest.raises(Exception) as e:
            test_options = {
                "timeout": "1ms",
            }

            df = util_and_resources.get_explicit_df(self.spark_session, **test_options)
            assert df.count() == 187

        assert "AmbiguousTimeoutException" in str(e.value)
        assert '"timeoutMs":1,' in str(e.value)
