import json
from tap_chargebee.streams.base import BaseChargebeeStream


class CustomersStream(BaseChargebeeStream):
    STREAM = 'customers'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'customer'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'common/customers'
    SORT_BY = 'updated_at'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/customers'.format(self.config.get('site'))

    def add_custom_fields(self, record: dict):
        """
        Adds custom fields to the record.
        """
        custom_fields = {}
        for key in record.keys():
            if "cf_" in key:
                custom_fields[key] = record[key]

        if custom_fields:
            record["custom_fields"] = json.dumps(custom_fields)

        return record
