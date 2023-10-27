import os

from tap_tester import connections, runner

from base import ChargebeeBaseTest
from tap_tester import LOGGER


class ChargebeeStartDateTest(ChargebeeBaseTest):

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_chargebee_start_date_test"

    def start_date_test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        self.start_date_1 = self.get_properties().get('start_date')
        if self.is_product_catalog_v1:
            self.start_date_2 = '2021-03-03T00:00:00Z'
        else:
            self.start_date_2 = '2021-06-22T00:00:00Z'

        start_date_1_epoch = self.dt_to_ts(self.start_date_1, self.START_DATE_FORMAT)
        start_date_2_epoch = self.dt_to_ts(self.start_date_2, self.START_DATE_FORMAT)

        self.start_date = self.start_date_1

        # WE ARE NOT ABLE TO GENERATE TEST DATA SO SKIPPING THREE STREAMS(orders, gifts, virtual_bank_accounts)
        expected_streams = self.expected_streams() - {'orders', 'gifts', 'virtual_bank_accounts', 'quotes'}

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

         ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        LOGGER.info("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        # Verify the total number of records replicated in sync 1 is greater than the number
        # of records replicated in sync 2
        self.assertGreater(sum(record_count_by_stream_1.values()), sum(record_count_by_stream_2.values()))

        for stream in expected_streams:

            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_bookmark_keys = self.expected_replication_keys()[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream).get('messages')
                                       if message.get('action') == 'upsert']

                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                # All streams are INCREMENTAL so no need of any condition

                # Expected bookmark key is one element in set so directly access it
                bookmark_keys_list_1 = [message.get('data').get(next(iter(expected_bookmark_keys))) for message in synced_records_1.get(stream).get('messages')
                                        if message.get('action') == 'upsert']
                bookmark_keys_list_2 = [message.get('data').get(next(iter(expected_bookmark_keys))) for message in synced_records_2.get(stream).get('messages')
                                        if message.get('action') == 'upsert']

                bookmark_key_sync_1 = set(bookmark_keys_list_1)
                bookmark_key_sync_2 = set(bookmark_keys_list_2)

                # Verify bookmark key values are greater than or equal to start date of sync 1
                for bookmark_key_value in bookmark_key_sync_1:
                    self.assertGreaterEqual(self.dt_to_ts(bookmark_key_value, self.RECORD_REPLICATION_KEY_FORMAT), start_date_1_epoch)

                # Verify bookmark key values are greater than or equal to start date of sync 2
                for bookmark_key_value in bookmark_key_sync_2:
                    self.assertGreaterEqual(self.dt_to_ts(bookmark_key_value, self.RECORD_REPLICATION_KEY_FORMAT), start_date_2_epoch)

                # Verify the number of records replicated in sync 1 is greater than or equal to the number
                # of records replicated in sync 2 for stream
                self.assertGreaterEqual(record_count_sync_1, record_count_sync_2)

                # Verify the records replicated in sync 2 were also replicated in sync 1
                self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

    def test_run(self):

        #Start date test Product Catalog version 1
        self.is_product_catalog_v1 = True
        self.start_date_test_run()

        #Start date test Product Catalog version 1
        self.is_product_catalog_v1 = False
        self.start_date_test_run()
