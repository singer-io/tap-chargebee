from tap_chargebee.streams.base import BaseChargebeeStream


class OrdersStream(BaseChargebeeStream):
    STREAM = 'orders'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'order'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'common/orders'
    SORT_BY = 'updated_at'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/orders'.format(self.config.get('site'))
