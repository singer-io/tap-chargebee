from tap_chargebee.streams.base import BaseChargebeeStream


class PromotionalCreditsStream(BaseChargebeeStream):
    TABLE = 'promotional_credits'
    ENTITY = 'promotional_credit'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'created_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['created_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['created_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'plan_model/promotional_credits'
    SORT_BY = None

    def __init__(self, config, state, catalog, client):
        BaseChargebeeStream.__init__(self, config, state, catalog, client)
        if self.config['item_model']:
            self.SCHEMA = 'item_model/promotional_credits'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/promotional_credits'.format(self.config.get('site'))
