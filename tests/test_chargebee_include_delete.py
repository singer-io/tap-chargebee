"""Test tap sync mode and metadata."""
import re

import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections

from base import ChargebeeBaseTest


class ChargebeeIncludeDeletedTest(ChargebeeBaseTest):
    """Test tap sync mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_chargebee_include_deleted_test"

    def setUp(self):
        self.include_deleted = None
        super().setUp()

    def get_properties(self):
        properties = super().get_properties()

        # include_deleted is an optional property for configuration
        if self.include_deleted is False:
            properties["include_deleted"] = 'false'

        return properties

    def run_sync(self, expected_streams):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs)

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        return runner.get_upserts_from_target_output()

    def run_include_deleted_test(self):
        """
        Testing that 2 sync have difference in data for stream invoices
        """
        # Expected stream is only invoices
        expected_streams = ["invoices"]

        # default value
        self.include_deleted = True

        # For include_delete true or not set
        synced_records_with_include_deleted_true = self.run_sync(
            expected_streams)

        deleted_status_for_include_deleted_true = [record["deleted"] for record in synced_records_with_include_deleted_true]
        
        # Verifying that deleted records are there before
        self.assertEqual(True, True in deleted_status_for_include_deleted_true)

        # For include_delete false

        self.include_deleted = False

        synced_records_with_include_deleted_false = self.run_sync(
            expected_streams)

        deleted_status_for_include_deleted_false = [record["deleted"] for record in synced_records_with_include_deleted_false]

        # Verifying that deleted records are not available
        self.assertEqual(True, (True not in deleted_status_for_include_deleted_false))

        # Compare with deleted records count with without deleted records count. With deleted records count must be higher.
        self.assertGreater(
            len(synced_records_with_include_deleted_true),
            len(synced_records_with_include_deleted_false)
        )

    def test_run(self):
    
        #Sync test for Product Catalog version 1
        self.product_catalog_v1 = True
        self.run_include_deleted_test()

        #Sync test for Product Catalog version 2
        self.product_catalog_v1 = False
        self.run_include_deleted_test()