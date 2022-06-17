import datetime
import pytz
from tap_chargebee.client import ChargebeeClient
from unittest import mock
from tap_chargebee.streams.events import EventsStream
import unittest

# mock transfrom and return record
def mock_transform(*args, **kwargs):
    return args[0]

@mock.patch("tap_chargebee.streams.events.EventsStream.transform_record", side_effect = mock_transform)
@mock.patch("tap_chargebee.client.ChargebeeClient.make_request")
@mock.patch("singer.write_records")
@mock.patch("tap_chargebee.streams.base.save_state")
@mock.patch('tap_chargebee.streams.base.datetime', mock.Mock(now=mock.Mock(return_value=datetime.datetime(2022, 1, 1, 5, 5, tzinfo=pytz.utc))))
class TestBookmarking(unittest.TestCase):
    """
        Test cases to verify we are setting minimum (now - 2 minutes) as bookmark
    """

    config = {
        "start_date": "2022-01-01T00:00:00Z",
        "api_key": "test_api_key",
        "site": "test-site",
        "item_model": None,
        "include_deleted": False
    }
    client = ChargebeeClient(config, include_deleted=False)
    events = EventsStream(config, {}, {}, client)

    def test_now_minus_2_minute_bookmark(self, mocked_save_state, mocked_records, mocked_make_request, mocked_transform_record):
        """
            Test case to verify we are setting (now - 2 min) as bookmark as we have max replication key greater than (now - 2 min)
        """
        mocked_make_request.return_value = {
            "list": [
                {"event": {"id": 1, "occurred_at": "2022-01-01T05:10:00.000000Z"}}]
        }
        self.events.sync_data()
        args, kwargs = mocked_save_state.call_args
        bookmark = args[0]
        self.assertEqual(bookmark.get("bookmarks").get("events").get("bookmark_date"), "2022-01-01T05:03:00Z")

    def test_max_replication_key_bookmark(self, mocked_save_state, mocked_records, mocked_make_request, mocked_transform_record):
        """
            Test case to verify we are setting (now - 2 min) as bookmark when we have max replication key lesser than (now - 2 min)
        """
        mocked_make_request.return_value = {
            "list": [
                {"event": {"id": 1, "occurred_at": "2022-01-01T05:02:00.000000Z"}}]
        }
        self.events.sync_data()
        args, kwargs = mocked_save_state.call_args
        bookmark = args[0]
        self.assertEqual(bookmark.get("bookmarks").get("events").get("bookmark_date"), "2022-01-01T05:03:00Z")
