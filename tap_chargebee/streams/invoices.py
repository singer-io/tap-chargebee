from tap_chargebee.streams.base import BaseChargebeeStream


class InvoicesStream(BaseChargebeeStream):
    TABLE = 'invoices'
    ENTITY = 'invoice'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['updated_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'plan_model/invoices'
    SORT_BY = 'updated_at'

    def __init__(self, config, state, catalog, client):
        BaseChargebeeStream.__init__(self, config, state, catalog, client)
        if self.config['item_model']:
            self.SCHEMA = 'item_model/invoices'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/invoices'.format(self.config.get('site'))
