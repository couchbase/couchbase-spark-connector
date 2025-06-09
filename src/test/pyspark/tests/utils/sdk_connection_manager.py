import logging
import os

from . import util_and_resources, columnar_manager, operational_cluster_manager

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SdkConnectionManager:
    default_bucket = util_and_resources.TRAVEL_SAMPLE_BUCKET
    curr_cluster_is_columnar = util_and_resources.get_curr_cluster_setup().df_type == util_and_resources.CB_COLUMNAR
    cluster_manager = None

    def __init__(self):
        if self.curr_cluster_is_columnar:
            self.cluster_manager = columnar_manager.ColumnarManager()
        else:
            self.cluster_manager = operational_cluster_manager.OperationalClusterManager()
        self.initialize_connection_if_needed()

    def initialize_connection_if_needed(self):
        connection_string = os.getenv('COUCHBASE_CONNECTION_STRING')
        username = os.getenv('COUCHBASE_USERNAME')
        password = os.getenv('COUCHBASE_PASSWORD')

        self.cluster_manager.initialize_connection_if_needed(
            connection_string=connection_string,
            username=username,
            password=password
        )

    def execute_query(self, query):
        return self.cluster_manager.execute_query(query)

    def create_scope(self, scope_name, bucket_name=default_bucket):
        self.execute_query(f'CREATE SCOPE `{bucket_name}`.`{scope_name}`')
        logger.info(f"Scope '{scope_name}' created successfully.")

    def drop_scope(self, scope_name, bucket_name=default_bucket):
        self.execute_query(f'DROP SCOPE `{bucket_name}`.`{scope_name}`')
        logger.info(f"Scope '{scope_name}' dropped successfully.")

    def create_collection(self, collection_name, scope_name, bucket_name=default_bucket):
        self.cluster_manager.create_collection(collection_name, scope_name, bucket_name)
        logger.info(f"Collection '{collection_name}' created under scope '{scope_name}' successfully.")

    def drop_collection(self, collection_name, scope_name, bucket_name=default_bucket):
        self.execute_query(f'DROP COLLECTION `{bucket_name}`.`{scope_name}`.`{collection_name}`')
        logger.info(f"Collection '{collection_name}' created under scope '{scope_name}' successfully.")

    def create_primary_index(self, collection_name, scope_name, bucket_name=default_bucket):
        self.cluster_manager.create_primary_index(collection_name, scope_name, bucket_name)

    def insert_doc(self, doc_id, value, collection_name, scope_name, bucket_name=default_bucket):
        self.cluster_manager.insert_doc(doc_id, value, collection_name, scope_name, bucket_name)
        logger.info(f"Doc inserted successfully in collection {collection_name}.")

    def delete_doc(self, doc_id, collection_name, scope_name=util_and_resources.INVENTORY_SCOPE,
                   bucket_name=default_bucket):
        self.execute_query(f'DELETE FROM `{bucket_name}`.`{scope_name}`.`{collection_name}` WHERE id = "{doc_id}"')
        logger.info(f"Deleted doc with ID: {doc_id} from collection {collection_name}.")

    def delete_all_docs(self, collection_name, scope_name, bucket_name=default_bucket):
        self.execute_query(f"DELETE FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`")
        logger.info(f"All documents deleted from collection '{collection_name}' in scope '{scope_name}'.")

    def get_list_of_completed_requests(self):
        return self.cluster_manager.get_list_of_completed_requests()

    def close(self):
        self.cluster_manager.close()
