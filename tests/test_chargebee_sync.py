"""Test tap sync mode and metadata."""
import re

from tap_tester import runner, menagerie, connections

from base import ChargebeeBaseTest


class ChargebeeSyncTest(ChargebeeBaseTest):
    """Test tap sync mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_chargebee_sync_test"

    def sync_test_run(self):
        """
        Testing that sync creates the appropriate catalog with valid metadata.
        • Verify that all fields and all streams have selected set to True in the metadata
        """
        conn_id = connections.ensure_connection(self)

        found_catalogs1 = self.run_and_verify_check_mode(conn_id)

        expected_streams = self.expected_streams()

        # table and field selection
        found_catalogs = [catalog for catalog in found_catalogs1
                                      if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id,found_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        self.assertGreater(sum(record_count_by_stream.values()), 0)

    def test_run(self):

        #Sync test for Product Catalog version 1
        self.is_product_catalog_v1 = True
        self.sync_test_run()

        #Sync test for Product Catalog version 2
        self.is_product_catalog_v1 = False
        self.sync_test_run()
