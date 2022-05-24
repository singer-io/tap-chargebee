from tap_tester import connections, runner, menagerie
from base import ChargebeeBaseTest

class ChargebeeAllFieldsTest(ChargebeeBaseTest):

    fields_to_remove = {
        'payment_sources': { # require 'token_id' for POST call
            'ip_address',
            'issuing_country',
            'bank_account',
            'amazon_payment',
            'paypal'
        },
        'plans': {
            'accounting_category4',
            'custom_fields', # added field in POST call but not reflected
            'shipping_frequency_period_unit', # added field in POST call but not reflected
            'event_based_addons', # added field in POST call but not reflected
            'tax_profile_id',
            'archived_at', # added field in POST call but not reflected
            'applicable_addons', # added field in POST call but not reflected
            'free_quantity_in_decimal',
            'shipping_frequency_period', # added field in POST call but not reflected
            'avalara_transaction_type', # configure Avatax for Communications
            'price_in_decimal',
            'account_code', # added field in POST call but not reflected
            'avalara_sale_type', # configure Avatax for Communications
            'tax_code',
            'attached_addons', # added field in POST call but not reflected
            'avalara_service_type', # configure Avatax for Communications
            'accounting_category3',
            'tiers', # added field in POST call but not reflected
            'taxjar_product_code', # configure Avatax for Communications
            'claim_url', # added field in POST call but not reflected
            'trial_end_action'
        },
        'addons': {
            'custom_fields', # added field in POST call but not reflected
            'tax_profile_id',
            'avalara_transaction_type', # configure Avatax for Communications
            'price_in_decimal', # Multi decimal feature is disabled
            'avalara_sale_type', # configure Avatax for Communications
            'tax_code',
            'accouting_category4', # added field in POST call but not reflected
            'avalara_service_type', # configure Avatax for Communications
            'taxjar_product_code', # TaxJar should be enabled
            'included_in_mrr', # enable Monthly Recurring Revenue
            'accouting_category3',
        },
        'transactions': {
            'reference_transaction_id',
            'reversal_transaction_id',
            'voided_at',
            'reversal_txn_id',
            'reference_number',
            'fraud_flag',
            'reference_authorization_id',
            'validated_at',
            'fraud_reason',
            'settled_at',
            'initiator_type',
            'authorization_reason',
            'three_d_secure',
            'merchant_reference_id',
            'linked_payments',
            'amount_capturable'
        },
        'subscriptions': {
            'custom_fields', # added field in POST call but not reflected
            'referral_info', # added field in POST call but not reflected
            'contract_term_billing_cycle_on_renewal', # Enable Contract terms feature
            'auto_close_invoices', # added field in POST call but not reflected
            'trial_end_action', # Enable Trial End Action feature
            'free_period', # added field in POST call but not reflected
            'event_based_addons', # added field in POST call but not reflected
            'gift_id', # added field in POST call but not reflected
            'has_scheduled_advance_invoices', # added field in POST call but not reflected
            'plan_quantity_in_decimal', # added field in POST call but not reflected
            'plan_unit_price_in_decimal', # added field in POST call but not reflected
            'create_pending_invoices', # added field in POST call but not reflected
            'plan_free_quantity_in_decimal', # added field in POST call but not reflected
            'offline_payment_method', # Enable offline_payment_method feature
            'pause_date', # added field in POST call but not reflected
            'override_relationship', # added field in POST call but not reflected
            'contract_term', # added field in POST call but not reflected
            'resume_date', # added field in POST call but not reflected
            'charged_event_based_addons', # added field in POST call but not reflected
            'plan_amount_in_decimal', # added field in POST call but not reflected
            'free_period_unit', # added field in POST call but not reflected
        },
        'promotional_credits': {
            'amount_in_decimal' # added field in POST call but not reflected
        },
        'credit_notes': {
            'voided_at', # added field in POST call but not reflected
            'line_item_tiers', # added field in POST call but not reflected
            'total_in_local_currency', # added field in POST call but not reflected
            'local_currency_code', # added field in POST call but not reflected
            'vat_number' # added field in POST call but not reflected
        },
        'invoices': {
            'voided_at', # added field in POST call but not reflected
            'line_item_tiers', # added field in POST call but not reflected
            'total_in_local_currency', # added field in POST call but not reflected
            'line_item_taxes', # added field in POST call but not reflected
            'local_currency_code', # added field in POST call but not reflected
            'void_reason_code', # added field in POST call but not reflected
            'next_retry_at', # added field in POST call but not reflected
            'payment_owner', # added field in POST call but not reflected
            'vat_number_prefix', # added field in POST call but not reflected
            'taxes', # added field in POST call but not reflected
            'sub_total_in_local_currency', # added field in POST call but not reflected
            'expected_payment_date' # added field in POST call but not reflected
        },
        'quotes': {
            'line_item_tiers', # added field in POST call but not reflected
            'contract_term_start', # added field in POST call but not reflected
            'subscription_id', # added field in POST call but not reflected
            'invoice_id', # added field in POST call but not reflected
            'contract_term_termination_fee', # added field in POST call but not reflected
            'contract_term_end', # added field in POST call but not reflected
            'vat_number_prefix', # added field in POST call but not reflected
            'discounts' # added field in POST call but not reflected
        },
        'coupons': {
            'included_in_mrr', # Enable Monthly Recurring Revenue setting
        },
        'events': {
            'user' # no POST call available
        },
        'customers': {
            'billing_date_mode', # added field in POST call but not reflected
            'auto_close_invoices', # Metered Billing must be enabled
            'exemption_details', # Configure Avatax for Communications
            'client_profile_id', # Configure Avatax for Communications
            'parent_account_access', # added field in POST call but not reflected
            'registered_for_gst', # added field in POST call but not reflected
            'customer_type', # Configure Avatax for Communications
            'billing_day_of_week_mode', # added field in POST call but not reflected
            'mrr',
            'billing_day_of_week', # added field in POST call but not reflected
            'vat_number_prefix',
            'use_default_hierarchy_settings', # added field in POST call but not reflected
            'referral_urls', # added field in POST call but not reflected
            'offline_payment_method', # must be enables
            'vat_number_validated_time', # added field in POST call but not reflected
            'fraud_flag', # added field in POST call but not reflected
            'relationship', # added field in POST call but not reflected
            'entity_code', # Configure Avatax for Sales
            'business_customer_without_vat_number', # Validate Vat
            'is_location_valid', # added field in POST call but not reflected
            'child_account_access', # added field in POST call but not reflected
            'exempt_number', # Configure Avatax for Sales
            'vat_number_status', # added field in POST call but not reflected
            'billing_date' # added field in POST call but not reflected
        }
    }

    def name(self):
        return 'chargebee_all_fields_test'

    def all_fields_test_run(self):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream.
        • verify all fields for each stream are replicated
        """

        # Skipping streams virtual_bank_accounts, gifts and orders as we are not able to generate data
        expected_streams = self.expected_streams() - {'virtual_bank_accounts', 'gifts', 'orders'}

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        # table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in catalog_entries:
            stream_id, stream_name = catalog["stream_id"], catalog["stream_name"]
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry["breadcrumb"][1] for md_entry in catalog_entry["metadata"]
                                          if md_entry["breadcrumb"] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()
        
        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_automatic_keys = expected_automatic_fields.get(stream, set())

                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                # remove some fields as data cannot be generated / retrieved
                fields = self.fields_to_remove.get(stream) or []
                for field in fields:
                    expected_all_keys.remove(field)

                self.assertSetEqual(expected_all_keys, actual_all_keys)

    def test_run(self):

        # All fields test for Product Catalog version 1
        self.product_catalog_v1 = True
        self.all_fields_test_run()

        # All fields test for Product Catalog version 2
        self.product_catalog_v1 = False
        self.all_fields_test_run()