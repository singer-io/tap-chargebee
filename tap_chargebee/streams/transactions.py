from tap_chargebee.streams.base import BaseChargebeeStream


class TransactionsStream(BaseChargebeeStream):
    STREAM = 'transactions'
    ENTITY = 'transaction'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'common/transactions'
    SORT_BY = 'updated_at'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/transactions'.format(self.config.get('site'))
