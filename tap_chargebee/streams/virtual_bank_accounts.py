from tap_chargebee.streams.base import BaseChargebeeStream


class VirtualBankAccountsStream(BaseChargebeeStream):
    STREAM = 'virtual_bank_accounts'
    ENTITY = 'virtual_bank_account'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'common/virtual_bank_accounts'
    SORT_BY = None

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/virtual_bank_accounts'.format(self.config.get('site'))
