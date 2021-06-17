"""Test tap sync mode and metadata."""
import re

from tap_tester import runner, menagerie, connections

from base import ChargebeeBaseTest


class ChargebeePaginationTest(ChargebeeBaseTest):
    """Test tap sync mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_chargebee_pagination_test"

    def setUp(self):
        self.include_deleted = None
        super().setUp()

    def get_properties(self):
        properties = super().get_properties()

        # include_deleted is an optional property for configuration
        if self.include_deleted is False:
            properties["include_deleted"] = self.include_deleted

        return properties

    def run_sync(self, expected_streams):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs)

        return self.run_and_verify_sync(conn_id)

    def test_run(self):
        """
        Testing that 2 sync have difference in data for stream invoices
        """
        # Expected stream is only invoices
        expected_streams = ["invoices"]

        synced_records_with_include_deleted_false = self.run_sync(
            expected_streams)

        # For include_delete true

        self.include_deleted = False

        synced_records_with_include_deleted_true = self.run_sync(
            expected_streams)

        self.assertGreater(sum(synced_records_with_include_deleted_false.values()), sum(
            synced_records_with_include_deleted_true.values()))