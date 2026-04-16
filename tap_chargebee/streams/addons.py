import json
from tap_chargebee.streams.base import BaseChargebeeStream
from .util import Util

class AddonsStream(BaseChargebeeStream):
    STREAM = 'addons'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'updated_at'
    KEY_PROPERTIES = ['id']
    ENTITY = 'addon'
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'plan_model/addons'
    SORT_BY = None

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/addons'.format(self.config.get('site'))

    def handle_deleted_items(self, records: dict):
        """
        Handle deleted records based on the include_deleted config.
        """
        deleted_records = []
        if self.include_deleted:
            for addon in Util.addons:
                deleted_records.append(addon)
        return deleted_records
