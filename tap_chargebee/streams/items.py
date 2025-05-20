from tap_chargebee.streams.base import BaseChargebeeStream


class ItemsStream(BaseChargebeeStream):
    STREAM = 'items'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'item'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'item_model/items'
    SORT_BY = 'updated_at'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/items'.format(self.config.get('site'))
