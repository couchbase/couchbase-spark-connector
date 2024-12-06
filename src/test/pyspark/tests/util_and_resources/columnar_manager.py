import logging

from couchbase_columnar.cluster import Cluster as ColumnarCluster
from couchbase_columnar.credential import Credential as ColumnarCredential
from src.test.pyspark.tests.util_and_resources import util_and_resources

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ColumnarManager:
    cluster = None

    def initialize_connection_if_needed(self, connection_string, username, password):
        try:
            cred = ColumnarCredential.from_username_and_password(username, password)
            self.cluster = ColumnarCluster.create_instance(connection_string, cred)
            logger.info("Executing columnar query....")
            self.cluster.execute_query(
                f"SELECT * FROM `{util_and_resources.TRAVEL_SAMPLE_BUCKET}`."
                f"`{util_and_resources.INVENTORY_SCOPE}`.`{util_and_resources.AIRLINE_COLLECTION}` LIMIT 1")

            logger.info("Columnar connection initialized successfully.")

        except Exception as e:
            logger.info(f"Error initializing Couchbase cluster connection: {e}")

    def execute_query(self, query):
        try:
            return self.cluster.execute_query(query)

        except Exception as e:
            logger.info(f"Error executing query: {e}")

    def create_collection(self, collection_name, scope_name, bucket_name):
        self.execute_query(
            f'CREATE COLLECTION `{bucket_name}`.`{scope_name}`.`{collection_name}` PRIMARY KEY (id: string)')

    def create_primary_index(self, collection_name, scope_name, bucket_name):
        logger.info("In columnar, primary index is based on primary key and we can't explicitly create it.")

    def insert_doc(self, doc_id, value, collection_name, scope_name, bucket_name):
        try:
            value["id"] = doc_id
            value = str(value).replace("'", '"')
            self.cluster.execute_query(
                f'INSERT INTO `{bucket_name}`.`{scope_name}`.`{collection_name}` ([{value}])')

        except Exception as e:
            logger.info(f"Error inserting doc in collection: {e}")

    def get_list_of_completed_requests(self):
        return self.execute_query("SELECT VALUE completed_requests()")

    def close(self):
        if self.cluster is not None:
            self.cluster.shutdown()
            logger.info("Columnar cluster connection closed.")
        else:
            logger.info("No active Columnar cluster connection to close.")
