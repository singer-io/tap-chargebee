import unittest
from unittest.mock import patch, MagicMock
from tap_chargebee.streams.base import BaseChargebeeStream
from tap_chargebee.streams.comments import CommentsStream
from tap_chargebee.streams.events import EventsStream
from tap_chargebee.client import ChargebeeClient
import json


class TestSyncMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {"start_date": "2017-01-01T00:00:00Z", "include_deleted": "false"}
        self.chargebee_client = ChargebeeClient(self.config)

    @patch("tap_chargebee.client.ChargebeeClient.get_offset_based_pages")
    def test_sync_no_pages(self, mock_get_pages):
        """
        Test sync method with no pages
        """
        base_instance = CommentsStream(
            config=self.config, state={}, catalog=None, client=self.chargebee_client
        )
        counter = base_instance.sync()
        base_instance.update_bookmark = MagicMock()

        base_instance.client.get_offset_based_pages.assert_called_once_with(
            "https://None.chargebee.com/api/v2/comments",
            "GET",
            "created_at",
            {"created_at[after]": 1483228798},
        )
        # Assert that update_bookmark is not called
        base_instance.update_bookmark.assert_not_called()

        self.assertEqual(counter, 0)

    @patch("tap_chargebee.streams.base.Transformer.transform", return_value={})
    @patch("tap_chargebee.client.ChargebeeClient.get_offset_based_pages")
    def test_sync_multiple_pages(self, mock_get_pages, mock_transform):
        """
        Test sync method with multiple pages
        """
        base_instance = CommentsStream(
            config=self.config, state={}, catalog=None, client=self.chargebee_client
        )
        base_instance.catalog = MagicMock()
        mock_get_pages.return_value = [
            [{"comment": {"created_at": 1700042444}}],
            [{"comment": {"created_at": 1700045644}}],
        ]

        counter = base_instance.sync()

        base_instance.client.get_offset_based_pages.assert_called_once_with(
            "https://None.chargebee.com/api/v2/comments",
            "GET",
            "created_at",
            {"created_at[after]": 1483228798},
        )
        # Assert that update_bookmark is called with the latest created_at value
        self.assertEqual(
            base_instance.state,
            {"bookmarks": {"comments": {"created_at": "2023-11-15T10:54:04Z"}}},
        )
        # Assert that transform is called twice
        self.assertEqual(counter, 2)


class TestAppendCustomFields(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "include_deleted": "false",
            "item_model": True,
        }

    def test_append_custom_fields_event_entity(self):
        """
        Test appendCustomFields method for event entity
        """
        base_instance = EventsStream(
            config=self.config, state={}, catalog=None, client=None
        )
        record = {
            "event_type": "subscription_created",
            "content": {
                "subscription": {
                    "cf_custom_field1": "value1",
                    "cf_custom_field2": "value2",
                }
            },
        }
        expected_record = {
            "event_type": "subscription_created",
            "content": {
                "subscription": {
                    "cf_custom_field1": "value1",
                    "cf_custom_field2": "value2",
                    "custom_fields": json.dumps(
                        {"cf_custom_field1": "value1", "cf_custom_field2": "value2"}
                    ),
                }
            },
            "custom_fields": "{}",
        }
        result = base_instance.appendCustomFields(record)
        self.assertEqual(result, expected_record)

    def test_append_custom_fields_non_event_entity(self):
        """
        Test appendCustomFields method for non-event entity
        """
        base_instance = CommentsStream(
            config=self.config, state={}, catalog=None, client=None
        )
        record = {"cf_custom_field1": "value1", "cf_custom_field2": "value2"}
        expected_record = {
            "cf_custom_field1": "value1",
            "cf_custom_field2": "value2",
            "custom_fields": json.dumps(
                {"cf_custom_field1": "value1", "cf_custom_field2": "value2"}
            ),
        }
        result = base_instance.appendCustomFields(record)
        self.assertEqual(result, expected_record)


class TestBookmakrMethods(unittest.TestCase):

    def setUp(self) -> None:
        self.config = {"start_date": "2017-01-01T00:00:00Z", "include_deleted": "false"}

    def test_update_bookmark_greater_value(self):
        """
        Test update_bookmark method with greater value
        """
        state = {"bookmarks": {"comments": {"created_at": "2019-01-01T00:00:00Z"}}}
        base_instance = CommentsStream(
            config=self.config, state=state, catalog=None, client=None
        )  # Replace with actual class name
        base_instance.update_bookmark("2021-02-01T00:00:00Z")
        # Assert that the bookmark is updated
        self.assertEqual(
            base_instance.state,
            {"bookmarks": {"comments": {"created_at": "2021-02-01T00:00:00Z"}}},
        )

    def test_update_bookmark_lower_value(self):
        """
        Test update_bookmark method with lower value
        """
        state = {"bookmarks": {"comments": {"created_at": "2026-01-01T00:00:00Z"}}}
        base_instance = CommentsStream(
            config=self.config, state=state, catalog=None, client=None
        )  # Replace with actual class name
        base_instance.update_bookmark("2011-02-01T00:00:00Z")
        # Assert that the bookmark is not updated
        self.assertEqual(
            base_instance.state,
            {"bookmarks": {"comments": {"created_at": "2026-01-01T00:00:00Z"}}},
        )

    def test_evaluate_bookmark_based_on_lookback(self):
        """
        Test evaluate_bookmark_based_on_lookback method
        """
        base_instance = CommentsStream(
            config=self.config, state={}, catalog=None, client=None
        )
        evaluate_bookmark = base_instance.evaluate_bookmark_based_on_lookback(
            "2021-02-01T00:00:00Z", 10
        )
        # Assert that the bookmark is evaluated based on lookback
        # 2021-02-01T00:00:00Z -> 1612137600 - 10 = 1612137590
        self.assertEqual(evaluate_bookmark, 1612137590)
