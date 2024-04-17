from tap_tester import connections, runner, menagerie
from base import ChargebeeBaseTest


class ChargebeeAllFieldsTest(ChargebeeBaseTest):

    # list of fields that are common between V1 and V2 for which data is not generated
    # we are removing this because we cannot find some fields in the UI, some fields require
    #   to enable Monthly Recurring Revenue setting, TaxJar, Contract terms feature,
    #   configure Avatax for Communications, Configure Avatax for Sales, Multi decimal feature
    fields_to_remove_common = {
        'promotional_credits': {'amount_in_decimal'}, # not found in the UI
        'invoices': { # not found in the UI
            'void_reason_code',
            'expected_payment_date',
            'voided_at',
            'payment_owner',
            'vat_number_prefix',
            'total_in_local_currency',
            'sub_total_in_local_currency',
            'local_currency_code',
            'next_retry_at',
            'einvoice'
        },
        'subscriptions': { # not found in the UI
            'create_pending_invoices',
            'free_period',
            'contract_term',
            'plan_free_quantity_in_decimal',
            'resume_date',
            'override_relationship',
            'auto_close_invoices',
            'contract_term_billing_cycle_on_renewal', # Enable Contract terms feature
            'plan_amount_in_decimal',
            'plan_quantity_in_decimal',
            'free_period_unit',
            'referral_info',
            'pause_date',
            'plan_unit_price_in_decimal',
            'trial_end_action', # Enable Trial End Action feature
            'changes_scheduled_at',
            'discounts',
            'event_based_addons'
        },
        'customers': { # not found in the UI
            'vat_number_validated_time',
            'referral_urls',
            'offline_payment_method',
            'entity_code', # Configure Avatax for Sales
            'billing_day_of_week_mode',
            'billing_date',
            'use_default_hierarchy_settings',
            'registered_for_gst',
            'exemption_details', # Configure Avatax for Communications
            'fraud_flag',
            'exempt_number', # Configure Avatax for Sales
            'vat_number_status',
            'billing_day_of_week',
            'parent_account_access',
            'child_account_access',
            'client_profile_id', # Configure Avatax for Communications
            'is_location_valid',
            'relationship',
            'billing_date_mode',
            'customer_type', # Configure Avatax for Communications
            'auto_close_invoices', # Metered Billing must be enabled
            'vat_number_prefix',
            'business_customer_without_vat_number', # Validate VAT
            'entity_identifier_standard',
            'is_einvoice_enabled',
            'entity_identifiers',
            'entity_identifier_scheme',
            'cf_people_id',
            'invoice_notes',
            'tax_providers_fields',
            'business_entity_id'

        },
        'credit_notes': { # not found in the UI
            'line_item_tiers',
            'vat_number_prefix',
            'total_in_local_currency',
            'sub_total_in_local_currency',
            'local_currency_code',
            'einvoice'
        },
        'payment_sources': { # not found in the UI
            'issuing_country',
            'paypal',
            'ip_address',
            'bank_account',
            'amazon_payment',
            'upi',
            'mandates'
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
            'reference_transaction_id',
            'iin',
            'last4'
        },
    }

    # fields to remove for V2, we cannot find some fields in the UI
    fields_to_remove_V2 = {
        'item_families': {'channel'},
        'item_prices': { # not found in the UI
            'free_quantity_in_decimal',
            'archivable',
            'tax_detail',
            'trial_end_action',
            'price_in_decimal',
            'accounting_detail',
            'shipping_period_unit',
            'shipping_period',
            'archived_at'
        },
        'invoices': { # not found in the UI
            'line_item_taxes',
            'taxes',
            'dunning_status',
            'vat_number'
        },
        'credit_notes': { # not found in the UI
            'voided_at',
            'vat_number',
            'discounts'
        },
        'items': { # not found in the UI
            'archivable',
            'gift_claim_redirect_url',
            'applicable_items',
            'usage_calculation',
            'included_in_mrr' # Enable Monthly Recurring Revenue setting
        },
        'coupons': { # not found in the UI
            'archived_at'
        },
        'customers': { # not found in the UI
            'backup_payment_source_id',
            'cf_company_id',
            'created_from_ip',
            'consolidated_invoicing',
            'billing_day_of_week',
            'vat_number'
        },
        'subscriptions': { # not found in the UI
            'cancel_reason',
            'start_date',
            'remaining_billing_cycles',
            'payment_source_id',
            'invoice_notes',
            'created_from_ip',
            'cancel_reason_code',
            'coupon',
            'coupons'
        },
        'transactions': { # not found in the UI
            'error_text',
            'reference_number',
            'error_code',
            'refunded_txn_id'
        }
    }

    # fields to remove for V1
    # we are removing this because we cannot find some fields in the UI, some fields require to enable
    #   Monthly Recurring Revenue setting, TaxJar, configure Avatax for Communications, Multi decimal feature
    fields_to_remove_V1 = {
        'coupons': {
            'included_in_mrr' # Enable Monthly Recurring Revenue setting
        },
        'addons': { # not found in the UI
            'avalara_service_type', # configure Avatax for Communications
            'accounting_category3',
            'taxjar_product_code', # TaxJar should be enabled
            'accounting_category4',
            'avalara_transaction_type', # configure Avatax for Communications
            'tiers',
            'tax_code',
            'price_in_decimal', # Multi decimal feature is disabled
            'included_in_mrr', # Enable Monthly Recurring Revenue setting
            'tax_profile_id',
            'avalara_sale_type' # configure Avatax for Communications
        },
        'quotes': { # not found in the UI
            'contract_term_start',
            'line_item_tiers',
            'vat_number_prefix',
            'invoice_id',
            'contract_term_termination_fee',
            'contract_term_end'
        },
        'events': {'user'},
        'invoices': {'line_item_tiers'},
        'plans': { # not found in the UI
            'avalara_service_type', # configure Avatax for Communications
            'account_code',
            'event_based_addons',
            'free_quantity_in_decimal',
            'taxjar_product_code', # configure Avatax for Communications
            'applicable_addons',
            'accounting_category4',
            'avalara_transaction_type', # configure Avatax for Communications
            'claim_url',
            'tiers',
            'tax_profile_id',
            'tax_code',
            'accounting_category3',
            'price_in_decimal', # Multi decimal feature is disabled
            'archived_at',
            'attached_addons',
            'avalara_sale_type', # configure Avatax for Sales
            'trial_end_action'
        },
        'subscriptions': { # not found in the UI
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

        untestable_streams_of_v2 = {'quotes'} # For V2, we have 0 records for 'quotes' stream
        # Skipping streams virtual_bank_accounts, gifts and orders as we are not able to generate data
        expected_streams = self.expected_streams() - {'virtual_bank_accounts', 'gifts', 'orders'}

        # skip quotes for product catalog V2
        if not self.is_product_catalog_v1:
            expected_streams = expected_streams - untestable_streams_of_v2

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
                if self.is_product_catalog_v1:
                    stream_fields_as_per_version = self.fields_to_remove_V1.get(stream, set())
                else:
                    stream_fields_as_per_version = self.fields_to_remove_V2.get(stream, set())
                # remove some fields as data cannot be generated / retrieved
                fields_to_remove = self.fields_to_remove_common.get(stream, set()) | stream_fields_as_per_version
                expected_all_keys = expected_all_keys - fields_to_remove

                self.assertSetEqual(expected_all_keys, actual_all_keys)

    def test_run(self):

        # All fields test for Product Catalog version 1
        self.is_product_catalog_v1 = True
        self.all_fields_test_run()

        # All fields test for Product Catalog version 2
        self.is_product_catalog_v1 = False
        self.all_fields_test_run()
