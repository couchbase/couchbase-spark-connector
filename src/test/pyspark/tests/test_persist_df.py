import re
import uuid
import pytest
from utils import util_and_resources

if util_and_resources.get_curr_cluster_setup().df_type == util_and_resources.CB_COLUMNAR:
    pytestmark = pytest.mark.skip(reason="Write operations are not supported for columnar dataframes")
else:
    pytestmark = pytest.mark.parametrize("df_type", [util_and_resources.CB_KV, util_and_resources.CB_QUERY])


@pytest.mark.usefixtures("class_fixture")
class TestPersistDf:
    spark_session = None
    test_options = {}

    @pytest.fixture(scope="class", autouse=True)
    def class_fixture(self, request, shared_spark_session, shared_sdk_connection):
        request.cls.spark_session = shared_spark_session

        unique_id = str(uuid.uuid4())
        request.cls.test_options = {
            "bucket": util_and_resources.TRAVEL_SAMPLE_BUCKET,
            "scope": "test_" + unique_id,
            "collection": "test_" + unique_id,
            "idFieldName": "id",
        }

        # Creating new scope & collection as new docs need to be added to collection
        # and this will change doc count for other tests
        shared_sdk_connection.create_scope(request.cls.test_options["scope"])
        shared_sdk_connection.create_collection(request.cls.test_options["collection"],
                                                request.cls.test_options["scope"])
        shared_sdk_connection.create_primary_index(request.cls.test_options["collection"],
                                                   request.cls.test_options["scope"])

        yield

        shared_sdk_connection.drop_scope(request.cls.test_options["scope"])

    @pytest.fixture(autouse=True)
    def cleanup_test_data(self, request, shared_sdk_connection):
        yield
        shared_sdk_connection.delete_all_docs(request.cls.test_options["collection"],
                                              request.cls.test_options["scope"])

    @pytest.mark.parametrize("curr_connection_id",
                             [util_and_resources.PRIMARY_CONNECTION_ID, util_and_resources.TEST_CONNECTION_ID])
    def test_persist_overwrite(self, df_type, curr_connection_id):
        df = util_and_resources.get_explicit_df(self.spark_session, curr_connection_id=curr_connection_id).limit(5)

        id_list = df.select("id").rdd.flatMap(lambda x: x).collect()

        (df.write.format(df_type)
         .options(**self.test_options)
         .save())

        (df.write.format(df_type)
         .options(**self.test_options)
         .mode("overwrite")
         .save())

        read_df = (self.spark_session.read.format(util_and_resources.CB_QUERY)
                   .options(**self.test_options)
                   .load())

        updated_df_id_list = read_df.select("id").rdd.flatMap(lambda x: x).collect()
        updated_df_id_list = list(map(int, updated_df_id_list))

        assert read_df.count() == 5
        assert updated_df_id_list == id_list

    def test_persist_ignore(self, df_type):
        df = util_and_resources.get_explicit_df(self.spark_session).limit(5)

        id_list = df.select("id").rdd.flatMap(lambda x: x).collect()

        (df.write.format(df_type)
         .options(**self.test_options)
         .save())

        (df.write.format(df_type)
         .options(**self.test_options)
         .mode("ignore")
         .save())

        read_df = (self.spark_session.read.format(util_and_resources.CB_QUERY)
                   .options(**self.test_options)
                   .load())

        updated_df_id_list = read_df.select("id").rdd.flatMap(lambda x: x).collect()
        updated_df_id_list = list(map(int, updated_df_id_list))

        read_df.show()
        assert read_df.count() == 5
        assert updated_df_id_list == id_list

    def test_persist_errorifexists(self, df_type):
        with pytest.raises(Exception) as e:
            df = util_and_resources.get_explicit_df(self.spark_session)

            (df.write.format(df_type)
             .options(**self.test_options)
             .save())

            (df.write.format(df_type)
             .options(**self.test_options)
             .mode("errorifexists")
             .save())

        assert "DocumentExistsException" in str(e.value)

    def test_persist_append(self, df_type):
        with pytest.raises(Exception) as e:
            df = util_and_resources.get_explicit_df(self.spark_session)

            (df.write.format(df_type)
             .options(**self.test_options)
             .save())

            (df.write.format(df_type)
             .options(**self.test_options)
             .mode("append")
             .save())

        if df_type == util_and_resources.CB_KV:
            assert "Unsupported SaveMode: Append" in str(e.value)
        else:
            assert "SaveMode.Append is not support" in str(e.value)

    @pytest.mark.parametrize("durability", ["majority", "majorityAndPersistToActive", "PersistToMajority", None])
    def test_timeout_and_durability_option(self, df_type, durability):
        with pytest.raises(Exception) as e:
            test_options = {
                "timeout": "1ms",
            }

            if df_type == util_and_resources.CB_KV:
                test_options["durability"] = durability

            df = util_and_resources.get_explicit_df(self.spark_session)

            (df.write.format(df_type)
             .options(**self.test_options)
             .options(**test_options)
             .save())

        assert "AmbiguousTimeoutException" in str(e.value)
        assert '"timeoutMs":1,' in str(e.value)
        if df_type == util_and_resources.CB_KV and durability is not None:
            sync_durability = re.sub(r'(?<!^)(?=[A-Z])', '_', durability).upper()
            assert f'"syncDurability":"{sync_durability}"' in str(e.value)
