import pytest

from utils import sdk_connection_manager
from utils import util_and_resources


@pytest.fixture(scope="session")
def shared_sdk_connection():
    conn = sdk_connection_manager.SdkConnectionManager()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def shared_spark_session():
    session = util_and_resources.create_spark_session()
    yield session
    session.stop()


# Ensure TestPysparkSession runs after all other tests because we need to create new spark context
# to test pyspark session creation and this will destroy shared_spark_session's context - SPARK-2243
def pytest_collection_modifyitems(items):
    end_tests = []
    rest = []

    for item in items:
        if "TestPysparkSession" in item.nodeid:
            end_tests.append(item)
        else:
            rest.append(item)

    items[:] = rest + end_tests
