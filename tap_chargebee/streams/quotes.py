from tap_chargebee.streams.base import BaseChargebeeStream


class QuotesStream(BaseChargebeeStream):
    STREAM = 'quotes'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'quote'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'common/quotes'
    SORT_BY = 'date'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/quotes'.format(self.config.get('site'))
