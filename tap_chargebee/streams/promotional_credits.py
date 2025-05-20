from tap_chargebee.streams.base import BaseChargebeeStream


class PromotionalCreditsStream(BaseChargebeeStream):
    STREAM = 'promotional_credits'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'created_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'promotional_credit'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['created_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'common/promotional_credits'
    SORT_BY = None

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/promotional_credits'.format(self.config.get('site'))
