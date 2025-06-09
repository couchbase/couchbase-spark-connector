import pytest
from utils import util_and_resources

PARTITION_ERROR = "If one of partitionColumn, partitionLowerBound, partitionUpperBound and partitionCount arguments are provided, then all must be"

if util_and_resources.get_curr_cluster_setup().df_type == util_and_resources.CB_COLUMNAR:
    pytestmark = pytest.mark.skip(reason="Partitioning is not supported for columnar dataframes")


@pytest.mark.usefixtures("class_fixture")
class TestPartition:
    spark_session = None

    @pytest.fixture(scope="class", autouse=True)
    def class_fixture(self, request, shared_spark_session):
        request.cls.spark_session = shared_spark_session

    def test_partition_basic(self):
        test_options = {
            "partitionColumn": "id",
            "partitionLowerBound": "1",
            "partitionUpperBound": "20000",
            "partitionCount": "10"
        }

        df = util_and_resources.get_explicit_df(self.spark_session, **test_options)

        df.printSchema()

        assert df.count() == 187
        assert df.rdd.getNumPartitions() == 10

        def check_partition(partition):
            assert len(list(partition)) != 187

        df.rdd.foreachPartition(check_partition)

    def test_partition_without_partition_column(self):
        with pytest.raises(Exception) as e:
            test_options = {
                "partitionLowerBound": "1",
                "partitionUpperBound": "1000",
                "partitionCount": "10"
            }

            util_and_resources.get_explicit_df(self.spark_session, **test_options)

        assert PARTITION_ERROR in str(e.value)

    def test_partition_without_partition_lower_bound(self):
        with pytest.raises(Exception) as e:
            test_options = {
                "partitionColumn": "id",
                "partitionUpperBound": "1000",
                "partitionCount": "10"
            }

            util_and_resources.get_explicit_df(self.spark_session, **test_options)

        assert PARTITION_ERROR in str(e.value)

    def test_partition_without_partition_upper_bound(self):
        with pytest.raises(Exception) as e:
            test_options = {
                "partitionColumn": "id",
                "partitionLowerBound": "1",
                "partitionCount": "10"
            }

            util_and_resources.get_explicit_df(self.spark_session, **test_options)

        assert PARTITION_ERROR in str(e.value)

    def test_partition_without_partition_count(self):
        with pytest.raises(Exception) as e:
            test_options = {
                "partitionColumn": "id",
                "partitionLowerBound": "1",
                "partitionUpperBound": "1000",
            }

            util_and_resources.get_explicit_df(self.spark_session, **test_options)

        assert PARTITION_ERROR in str(e.value)
