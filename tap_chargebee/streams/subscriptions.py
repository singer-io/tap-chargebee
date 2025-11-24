import json
from tap_chargebee.streams.base import BaseChargebeeStream


class SubscriptionsStream(BaseChargebeeStream):
    STREAM = 'subscriptions'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'subscription'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'plan_model/subscriptions'
    SORT_BY = 'updated_at'

    def __init__(self, config, state, catalog, client):
        BaseChargebeeStream.__init__(self, config, state, catalog, client)
        if self.config['item_model']:
            self.SCHEMA = 'item_model/subscriptions'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/subscriptions'.format(self.config.get('site'))

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
