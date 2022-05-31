from tap_tester import connections, runner, menagerie
from base import ChargebeeBaseTest

class ChargebeeAllFieldsTest(ChargebeeBaseTest):

    # fields that are common between V1 and V2
    fields_to_remove_common = {
        'promotional_credits': {'amount_in_decimal'},
        'invoices': {
            'void_reason_code',
            'expected_payment_date',
            'voided_at',
            'payment_owner',
            'line_item_tiers',
            'vat_number_prefix',
            'total_in_local_currency',
            'sub_total_in_local_currency',
            'local_currency_code',
            'next_retry_at'
        },
        'subscriptions': {
            'create_pending_invoices',
            'free_period',
            'contract_term',
            'plan_free_quantity_in_decimal',
            'resume_date',
            'override_relationship',
            'auto_close_invoices',
            'contract_term_billing_cycle_on_renewal',
            'plan_amount_in_decimal',
            'plan_quantity_in_decimal',
            'has_scheduled_advance_invoices',
            'free_period_unit',
            'referral_info',
            'pause_date',
            'plan_unit_price_in_decimal',
            'trial_end_action'
        },
        'customers': {
            'vat_number_validated_time',
            'referral_urls',
            'offline_payment_method',
            'entity_code',
            'billing_day_of_week_mode',
            'billing_date',
            'use_default_hierarchy_settings',
            'registered_for_gst',
            'exemption_details',
            'fraud_flag',
            'exempt_number',
            'vat_number_status',
            'billing_day_of_week',
            'parent_account_access',
            'child_account_access',
            'client_profile_id',
            'is_location_valid',
            'relationship',
            'billing_date_mode',
            'customer_type',
            'mrr',
            'auto_close_invoices',
            'vat_number_prefix',
            'business_customer_without_vat_number'
        },
        'credit_notes': {
            'line_item_tiers',
            'vat_number_prefix',
            'total_in_local_currency',
            'sub_total_in_local_currency',
            'local_currency_code'
        },
        'payment_sources': {
            'issuing_country',
            'paypal',
            'ip_address',
            'bank_account',
            'amazon_payment'
        },
        'transactions': {
            'fraud_flag',
            'authorization_reason',
            'voided_at',
            'reversal_txn_id',
            'initiator_type',
            'linked_payments',
            'three_d_secure',
            'merchant_reference_id',
            'settled_at',
            'reference_authorization_id',
            'reversal_transaction_id',
            'validated_at',
            'fraud_reason',
            'amount_capturable',
            'reference_transaction_id'
        },
    }
    # fields for V2
    fields_to_remove_V2 = {
        'item_prices': {
            'free_quantity_in_decimal',
            'archivable',
            'tax_detail',
            'billing_cycles',
            'trial_end_action',
            'price_in_decimal',
            'accounting_detail',
            'shipping_period_unit',
            'shipping_period',
            'archived_at'
        },
        'invoices': {
            'line_item_discounts',
            'line_item_taxes',
            'taxes',
            'discounts',
            'dunning_status',
            'vat_number'
        },
        'credit_notes': {
            'voided_at',
            'vat_number',
            'discounts'
        },
        'items': {
            'archivable',
            'gift_claim_redirect_url',
            'applicable_items',
            'redirect_url',
            'usage_calculation',
            'included_in_mrr'
        },
        'coupons': {
            'invoice_notes',
            'meta_data',
            'archived_at'
        },
        'customers': {
            'backup_payment_source_id',
            'meta_data',
            'cf_company_id',
            'custom_fields',
            'created_from_ip',
            'consolidated_invoicing',
            'billing_day_of_week',
            'vat_number'
        },
        'subscriptions': {
            'cancel_reason',
            'start_date',
            'meta_data',
            'remaining_billing_cycles',
            'payment_source_id',
            'custom_fields',
            'item_tiers',
            'invoice_notes',
            'created_from_ip',
            'cancel_reason_code',
            'coupon',
            'coupons'
        },
        'transactions': {
            'error_text',
            'reference_number',
            'error_code',
            'refunded_txn_id'
        },
        'promotional_credits': {
            'reference'
        },
        'events': {
            'user'
        }
    }
    # fields for V1
    fields_to_remove_V1 = {
        'coupons': {
            'included_in_mrr'
        },
        'addons': {
            'avalara_service_type',
            'accouting_category1',
            'accouting_category3',
            'taxjar_product_code',
            'accouting_category4',
            'avalara_transaction_type',
            'tiers',
            'accouting_category2',
            'tax_code',
            'price_in_decimal',
            'included_in_mrr',
            'tax_profile_id',
            'avalara_sale_type'
        },
        'quotes': {
            'contract_term_start',
            'line_item_tiers',
            'vat_number_prefix',
            'invoice_id',
            'contract_term_termination_fee',
            'contract_term_end'
        },
        'plans': {
            'avalara_service_type',
            'account_code',
            'event_based_addons',
            'free_quantity_in_decimal',
            'taxjar_product_code',
            'applicable_addons',
            'accounting_category4',
            'avalara_transaction_type',
            'claim_url',
            'tiers',
            'tax_profile_id',
            'tax_code',
            'accounting_category3',
            'price_in_decimal',
            'archived_at',
            'attached_addons',
            'avalara_sale_type',
            'trial_end_action'
        },
        'subscriptions': {
            'offline_payment_method',
            'gift_id'
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

        version = 'V1' if self.product_catalog_v1 else 'V2'
        untestable_streams = {'quotes'} # For V2, we have 0 records for 'quotes' stream
        # Skipping streams virtual_bank_accounts, gifts and orders as we are not able to generate data
        expected_streams = self.expected_streams() - {'virtual_bank_accounts', 'gifts', 'orders'}

        # skip quotes for product catalog V2
        if not self.product_catalog_v1:
            expected_streams = expected_streams - untestable_streams

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

                # get fields to remove for the version
                stream_fields_as_per_version = self.fields_to_remove_V1.get(stream, set()) if self.product_catalog_v1 \
                    else self.fields_to_remove_V2.get(stream, set())
                # remove some fields as data cannot be generated / retrieved
                fields = self.fields_to_remove_common.get(stream, set()) | stream_fields_as_per_version
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