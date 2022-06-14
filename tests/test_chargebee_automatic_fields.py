from tap_tester import connections,runner
from base import ChargebeeBaseTest

class ChargebeeAutomaticFieldsTest(ChargebeeBaseTest):

    def name(self):
        return "chargebee_automatic_fields_test"

    def automatic_fields_test_run(self):
        """
        Testing that all the automatic fields are replicated despite de-selecting them
        - Verify that only the automatic fields are sent to the target.
        - Verify that all replicated records have unique primary key values.
        """

        untestable_streams = {'quotes'} # For V2, we have 0 records for 'quotes' stream
        # Skipping streams virtual_bank_accounts, gifts and orders as we are not able to generate data
        expected_streams = self.expected_streams() - {'virtual_bank_accounts', 'gifts', 'orders'}

        # skip quotes for product catalog V2
        if not self.is_product_catalog_v1:
            expected_streams = expected_streams - untestable_streams

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select all streams and no fields within streams
        self.perform_and_verify_table_and_field_selection(conn_id, found_catalogs, select_all_fields=False)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_keys = self.expected_automatic_fields().get(stream)

                # collect actual values
                messages = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in messages['messages']]

                # check if the stream has collected some records
                self.assertGreater(record_count_by_stream.get(stream, 0), 0)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Verify we did not duplicate any records across pages
                records_pks_list = [tuple([message.get('data').get(primary_key) for primary_key in expected_primary_keys])
                                           for message in messages.get('messages')]
                self.assertCountEqual(records_pks_list, set(records_pks_list),
                                      msg="We have duplicate records for {}".format(stream))

    def test_run(self):

        # Automatic fields test for Product Catalog v1
        self.is_product_catalog_v1 = True
        self.automatic_fields_test_run()

        # Automatic fields test for Product Catalog v2
        self.is_product_catalog_v1 = False
        self.automatic_fields_test_run()