from tap_chargebee.streams.base import BaseChargebeeStream


class UnbilledCharges(BaseChargebeeStream):
    TABLE = 'unbilled_charges'
    ENTITY = 'unbilled_charge'
    REPLICATION_METHOD = 'FULL_TABLE'
    REPLICATION_KEY = 'id'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = []
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = []
    INCLUSION = 'available'
    API_METHOD = 'GET'

    def get_url(self):
        return 'https://{}/api/v2/unbilled_charges'.format(self.config.get('full_site'))
