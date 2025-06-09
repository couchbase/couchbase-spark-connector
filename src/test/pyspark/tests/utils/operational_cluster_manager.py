import logging
from datetime import timedelta

from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from couchbase.management.queries import CreatePrimaryQueryIndexOptions

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class OperationalClusterManager:
    cluster = None

    def initialize_connection_if_needed(self, connection_string, username, password):
        try:
            options = ClusterOptions(PasswordAuthenticator(username, password))
            options.apply_profile('wan_development')
            self.cluster = Cluster.connect(connection_string, options)

            self.cluster.wait_until_ready(timedelta(seconds=30))

            logger.info("Operational cluster connection initialized successfully.")

        except Exception as e:
            logger.info(f"Error initializing Couchbase cluster connection: {e}")

    def execute_query(self, query):
        try:
            query_result = self.cluster.query(query).execute()
            self.cluster.wait_until_ready(timedelta(seconds=30))
            return query_result

        except Exception as e:
            logger.info(f"Error executing query: {e}")

    def create_collection(self, collection_name, scope_name, bucket_name):
        self.execute_query(f'CREATE COLLECTION `{bucket_name}`.`{scope_name}`.`{collection_name}`')

    def create_primary_index(self, collection_name, scope_name, bucket_name):
        try:
            self.cluster.query_indexes().create_primary_index(
                bucket_name,
                CreatePrimaryQueryIndexOptions(
                    scope_name=scope_name,
                    collection_name=collection_name,
                    ignore_if_exists=True)
            )

            logger.info(
                f"Primary index created successfully on collection '{collection_name}' in scope '{scope_name}'.")

        except Exception as e:
            logger.info(f"Error creating primary index: {e}")

    def insert_doc(self, doc_id, value, collection_name, scope_name, bucket_name):
        try:
            self.cluster.bucket(bucket_name).scope(scope_name).collection(collection_name).insert(doc_id, value)

        except Exception as e:
            logger.info(f"Error inserting doc in collection: {e}")

    def get_list_of_completed_requests(self):
        return self.execute_query("SELECT * FROM system:completed_requests")

    def close(self):
        if self.cluster is not None:
            self.cluster.close()
            logger.info("Operational cluster connection closed.")
        else:
            logger.info("No active Operational cluster connection to close.")
