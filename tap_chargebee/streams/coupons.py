from tap_chargebee.streams.base import BaseChargebeeStream


class CouponsStream(BaseChargebeeStream):
    TABLE = 'coupons'
    ENTITY = 'coupon'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['updated_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SORT_BY = 'created_at' # https://apidocs.chargebee.com/docs/api/coupons#list_coupons_sort_by


    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/coupons'.format(self.config.get('site'))
