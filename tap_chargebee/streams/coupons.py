from tap_chargebee.streams.base import BaseChargebeeStream
from .util import Util

class CouponsStream(BaseChargebeeStream):
    STREAM = 'coupons'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'coupon'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'plan_model/coupons'
    SORT_BY = 'created_at'

    def __init__(self, config, state, catalog, client):
        BaseChargebeeStream.__init__(self, config, state, catalog, client)
        if self.config['item_model']:
            self.SCHEMA = 'item_model/coupons'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/coupons'.format(self.config.get('site'))

    def handle_deleted_items(self, records: dict):
        """
        Handle deleted records based on the include_deleted config.
        """
        deleted_records = []
        if self.include_deleted:
            for coupon in Util.coupons:
                deleted_records.append(coupon)
        return deleted_records