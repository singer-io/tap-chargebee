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
    SORT_BY = 'updated_at' # https://apidocs.chargebee.com/docs/api/invoices#list_invoices_sort_by

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/invoices'.format(self.config.get('site'))
