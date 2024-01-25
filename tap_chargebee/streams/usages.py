import singer

from .subscriptions import SubscriptionsStream

from dateutil.parser import parse
from tap_framework.config import get_config_start_date
from tap_chargebee.state import get_last_record_value_for_table, incorporate, \
    save_state

from tap_chargebee.streams.base import BaseChargebeeStream


LOGGER = singer.get_logger()

class UsagesStream(BaseChargebeeStream):
    TABLE = 'usages'
    ENTITY = 'usage'
    KEY_PROPERTIES = ['id']
    SELECTED_BY_DEFAULT = True
    REPLICATION_METHOD = "FULL"
    BOOKMARK_PROPERTIES = ['updated_at']
    VALID_REPLICATION_KEYS = ['updated_at']
    INCLUSION = 'available'
    API_METHOD = 'GET'
    _already_checked_subscription = []
    sync_data_for_child_stream = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.PARENT_STREAM_INSTANCE = SubscriptionsStream(*args, **kwargs)

    def get_url(self):
        return 'https://{}/api/v2/usages'.format(self.config.get('full_site'))

    def sync_data(self):
        table = self.TABLE
        api_method = self.API_METHOD
        done = False

        # Attempt to get the bookmark date from the state file (if one exists and is supplied).
        LOGGER.info('Attempting to get the most recent bookmark_date for entity {}.'.format(self.ENTITY))
        bookmark_date = get_last_record_value_for_table(self.state, table, 'bookmark_date')

        # If there is no bookmark date, fall back to using the start date from the config file.
        if bookmark_date is None:
            LOGGER.info('Could not locate bookmark_date from STATE file. Falling back to start_date from config.json instead.')
            bookmark_date = get_config_start_date(self.config)
        else:
            bookmark_date = parse(bookmark_date)

        # Convert bookmarked start date to POSIX.
        bookmark_date_posix = int(bookmark_date.timestamp())

        sync_failures = False

        # Gets parent stream data
        to_write = []
        # total = 0
        for subscription in self.PARENT_STREAM_INSTANCE.sync_parent_data():
            # Sets the url params
            subscription_id = subscription["subscription"]["id"]
            if subscription_id in self._already_checked_subscription:
                continue

            params = {
                'subscription_id[is]': subscription_id,
            }
            self._already_checked_subscription.append(subscription_id)

            # Gets the data
            try:
                response = self.client.make_request(self.get_url(), api_method, params=params)
            except Exception as e:
                response = {}

            for obj in response.get('list', []):
                to_write.append(obj.get("usage"))

        with singer.metrics.record_counter(endpoint=table) as ctr:
            singer.write_records(table, to_write)
            ctr.increment(amount=len(to_write))

        # Writes the data
