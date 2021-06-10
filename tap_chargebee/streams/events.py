from tap_chargebee.streams.base import BaseChargebeeStream


class EventsStream(BaseChargebeeStream):
    TABLE = 'events'
    ENTITY = 'event'
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEY = 'occurred_at'
    KEY_PROPERTIES = ['id']
    BOOKMARK_PROPERTIES = ['occurred_at']
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = ['occurred_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    SCHEMA = 'common/events'
    SORT_BY = 'occurred_at'

    def get_url(self):
        return 'https://{}.chargebee.com/api/v2/events'.format(self.config.get('site'))
