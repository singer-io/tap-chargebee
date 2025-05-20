from tap_chargebee.streams.base import BaseChargebeeStream


class ItemFamiliesStream(BaseChargebeeStream):
    STREAM = 'item_families'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'item_family'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'item_model/item_families'
    SORT_BY = None

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/item_families'.format(self.config.get('site'))
