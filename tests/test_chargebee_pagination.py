"""Test tap sync mode and metadata."""
import re
import os
import requests

from tap_tester import runner, menagerie, connections

from base import ChargebeeBaseTest


class ChargebeePaginationTest(ChargebeeBaseTest):
    """Test tap sync mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_chargebee_pagination_test"

    def generate_events(self):
        # Generate events for product catalog v1
        url = 'https://{}.chargebee.com/api/v1/customers/cbdemo_dave'.format(os.getenv("TAP_CHARGEBEE_SITE"))
        payload = 'first_name=Dave'
        # Update customer 20 times which will generate 20 events
        product_v1_api_key = os.getenv("TAP_CHARGEBEE_API_KEY")
        for index in range(20):
            requests.post(url=url, data=payload, auth=(product_v1_api_key,''))

        # Generate events for product catalog v2
        url = 'https://{}.chargebee.com/api/v2/customers/cbdemo_carol'.format(os.getenv("TAP_CHARGEBEE_SITE_V2"))
        payload = 'first_name=Carol'
        # Update customer 20 times which will generate 20 events
        product_v2_api_key = os.getenv("TAP_CHARGEBEE_API_KEY_V2")
        for index in range(20):
            requests.post(url=url, data=payload, auth=(product_v2_api_key,''))


    def pagination_test_run(self):
        """
        Testing that sync creates the appropriate catalog with valid metadata.
        • Verify that all fields and all streams have selected set to True in the metadata
        """
        page_size = 10 # Page size for events
        conn_id = connections.ensure_connection(self)

        # Expected stream is only events
        expected_streams = ["events"]
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                                      if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id,test_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync = record_count_by_stream.get(stream, 0)
                primary_keys_list = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                     for message in synced_records.get(stream).get('messages')
                                     if message.get('action') == 'upsert']

                # verify records are more than page size so multiple page is working
                self.assertGreater(record_count_sync, page_size)

                if record_count_sync > page_size:
                    primary_keys_list_1 = primary_keys_list[:page_size]
                    primary_keys_list_2 = primary_keys_list[page_size:2*page_size]

                    primary_keys_page_1 = set(primary_keys_list_1)
                    primary_keys_page_2 = set(primary_keys_list_2)

                    #Verify by private keys that data is unique for page
                    self.assertTrue(primary_keys_page_1.isdisjoint(primary_keys_page_2))

    def test_run(self):
        # generate two events for both version so it will make more than 100 evenets in last 90 days
        self.generate_events()

        #Pagination test for Product Catalog version 1
        self.is_product_catalog_v1 = True
        self.pagination_test_run()

        #Pagintaion test for Product Catalog version 2
        self.is_product_catalog_v1 = False
        self.pagination_test_run()
