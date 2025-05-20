import json
from tap_chargebee.streams.base import BaseChargebeeStream
from .util import Util

class EventsStream(BaseChargebeeStream):
    STREAM = 'events'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'occurred_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'event'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['occurred_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'plan_model/events'
    SORT_BY = 'occurred_at'

    def __init__(self, config, state, catalog, client):
        BaseChargebeeStream.__init__(self, config, state, catalog, client)
        if self.config['item_model']:
            self.SCHEMA = 'item_model/events'

    def handle_deleted_items(self, records):
        if self.include_deleted:
            # Parse "event_type" from events records and collect deleted plan/addon/coupon from events
            # Ref: https://github.com/singer-io/tap-chargebee/pull/58/files#r666092906
            for record in records:
                event = record[self.ENTITY]
                if event["event_type"] == 'plan_deleted':
                    Util.plans.append({'plan': event['content']['plan']})
                elif event['event_type'] == 'addon_deleted':
                    Util.addons.append({'addon': event['content']['addon']})
                elif event['event_type'] == 'coupon_deleted':
                    Util.coupons.append({'coupon': event['content']['coupon']})

        return super().handle_deleted_items(records)

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/events'.format(self.config.get('site'))

    def add_custom_fields(self, record):

        listOfCustomFieldObj = ['addon', 'plan', 'subscription', 'customer']
        custom_fields = {}
        event_custom_fields = {}
        if self.ENTITY == 'event':
            # payment_source_added -> payment_source, customer_created -> customer
            content_obj = record['event_type'].rsplit("_", 1)[0]


            if content_obj in listOfCustomFieldObj:
                event_custom_fields = {
                    k: v for k, v in record['content'][content_obj].items() if 'cf_' in k
                }
                record['content'][content_obj]['custom_fields'] = json.dumps(event_custom_fields) 

        for key in record.keys():
            if "cf_" in key:
                custom_fields[key] = record[key]

        if custom_fields:
            record['custom_fields'] = json.dumps(custom_fields)

        return record