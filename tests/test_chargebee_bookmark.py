from copy import deepcopy
from datetime import timedelta, datetime
from tap_tester import connections, runner, menagerie
from base import ChargebeeBaseTest
import dateutil.parser

class ChargebeeBookmarkTest(ChargebeeBaseTest):

    def name(self):
        return "chargebee_bookmark_test"

    def get_max_replication_value(self, records, replication_key):
        """
            Return maximum replication value for the stream
        """
        max_val = records[0].get(replication_key)
        for record in records[1:]:
            max_val = max(max_val, record.get(replication_key))
        return max_val

    def get_simulated_states(self, state, records):
        """
            Subtract 12 hours from all bookmarks in the state file
        """
        new_state = deepcopy(state)
        for stream_name, bookmark in new_state.get("bookmarks").items():
            stream_records = [record.get('data') for record in records.get(stream_name, {}).get('messages', [])
                              if record.get('action') == 'upsert']
            # as these streams are skipped the state file will contain the start date as bookmark
            if stream_name in self.streams_to_skip | {'quotes'}:
                bookmark_date = bookmark["bookmark_date"]
            else:
                bookmark_date = self.get_max_replication_value(stream_records, list(self.expected_replication_keys().get(stream_name))[0])
            new_bookmark_date = dateutil.parser.parse(bookmark_date) - timedelta(hours=12)
            bookmark["bookmark_date"] = datetime.strftime(new_bookmark_date, self.BOOKMARK_FORMAT)

        return new_state

    def bookmark_test_run(self):
        """
        Testing that the bookmarking for the tap works as expected
        - Verify for each incremental stream you can do a sync which records bookmarks
        - Verify that a bookmark doesn't exist for full table streams.
        - Verify the bookmark is the max value sent to the target for the a given replication key.
        - Verify 2nd sync respects the bookmark
        - All data of the 2nd sync is >= the bookmark from the first sync
        - The number of records in the 2nd sync is less then the first
        """

        untestable_streams = {'quotes'} # For V2, we have 0 records for 'quotes' stream
        # Skipping streams virtual_bank_accounts, gifts and orders as we are not able to generate data
        self.streams_to_skip = {'virtual_bank_accounts', 'gifts', 'orders'}
        expected_streams = self.expected_streams() - self.streams_to_skip

        # skip quotes for product catalog V2
        if not self.is_product_catalog_v1:
            expected_streams = expected_streams - untestable_streams

        expected_replication_keys = self.expected_replication_keys()

        ################################# First Sync #########################################

        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.perform_and_verify_table_and_field_selection(conn_id, found_catalogs)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ####################### Update State Between Syncs #########################

        new_states = self.get_simulated_states(first_sync_bookmarks, first_sync_records)
        menagerie.set_state(conn_id, new_states)

        ################################# Second Sync #########################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ########################### Test by Stream ###########################################

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                first_sync_messages = [record.get('data') for record in first_sync_records.get(stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in second_sync_records.get(stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']

                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                if self.is_incremental(stream):

                    # collect information specific to incremental streams from syncs 1 & 2
                    # Tap is using key as 'bookmark_date' while writing the states
                    replication_key = 'bookmark_date'
                    record_replication_key = list(expected_replication_keys[stream])[0]
                   
                    first_bookmark_value = first_bookmark_key_value.get(replication_key)
                    second_bookmark_value = second_bookmark_key_value.get(replication_key)

                    first_bookmark_value_ts = self.dt_to_ts(first_bookmark_value, self.BOOKMARK_FORMAT)
                    second_bookmark_value_ts = self.dt_to_ts(second_bookmark_value, self.BOOKMARK_FORMAT)

                    simulated_bookmark_value = self.dt_to_ts(new_states['bookmarks'][stream][replication_key], self.BOOKMARK_FORMAT)

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_value)

                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    # As we have implemented (now - 2 minutes) as bookmark, thus, bookmark will not be same for both syncs
                    # self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                    for record in first_sync_messages:
                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = self.dt_to_ts(record.get(record_replication_key), self.RECORD_REPLICATION_KEY_FORMAT)
                        self.assertLessEqual(replication_key_value, first_bookmark_value_ts,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = self.dt_to_ts(record.get(record_replication_key), self.RECORD_REPLICATION_KEY_FORMAT)
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                            msg="Second sync records do not respect the previous bookmark.")

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(replication_key_value, second_bookmark_value_ts,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                    # verify that you get less data the 2nd time around
                    self.assertLess(second_sync_count, first_sync_count,
                        msg="second sync didn't have less records, bookmark usage not verified")

                else:
                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

    def test_run(self):

        # Bookmark test for Product Catalog v1
        self.is_product_catalog_v1 = True
        self.bookmark_test_run()

        # Bookmark test for Product Catalog v2
        self.is_product_catalog_v1 = False
        self.bookmark_test_run()