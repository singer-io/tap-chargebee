import unittest
from unittest.mock import patch, MagicMock, mock_open
from tap_chargebee.streams.comments import CommentsStream


class TestLoadSharedSchemaMethods(unittest.TestCase):

    def setUp(self):
        self.config = {"start_date": "2017-01-01T00:00:00Z", "include_deleted": "false"}
        self.base_instance = CommentsStream(
            config=self.config, state={}, catalog=None, client=None
        )  # Replace with actual class name
        self.base_instance.config = {"item_model": False}

    @patch("tap_chargebee.streams.base.os")
    @patch(
        "tap_chargebee.streams.base.open",
        new_callable=mock_open,
        read_data='{"key": "value"}',
    )
    def test_load_shared_schema_ref(self, mock_open, mock_os):
        """
        Test load_shared_schema_ref method
        """
        mock_os.listdir.return_value = ["quotes.json", "gifts.json", "orders.json"]
        mock_os.path.isfile.side_effect = [True, True, True]

        result = self.base_instance.load_shared_schema_ref("common")
        self.assertEqual(mock_open.call_count, 3)
        self.assertEqual(
            result,
            {
                "quotes.json": {"key": "value"},
                "gifts.json": {"key": "value"},
                "orders.json": {"key": "value"},
            },
        )

    @patch("tap_chargebee.streams.base.os")
    @patch(
        "tap_chargebee.streams.base.open",
        new_callable=mock_open,
        read_data='{"key": "value"}',
    )
    def test_load_shared_schema_refs_with_item_model(self, mock_open, mock_os):
        """
        Test load_shared_schema_refs method with item_model set to True
        """
        self.base_instance.config["item_model"] = True
        self.base_instance.load_shared_schema_ref = MagicMock(
            return_value={"schema1.json": {"key": "value"}}
        )

        result = self.base_instance.load_shared_schema_refs()

        self.assertEqual(self.base_instance.load_shared_schema_ref.call_count, 2)
        self.base_instance.load_shared_schema_ref.assert_any_call("common")
        self.base_instance.load_shared_schema_ref.assert_any_call("item_model")
        self.assertEqual(
            result, {"schema1.json": {"key": "value"}, "schema1.json": {"key": "value"}}
        )

    @patch("tap_chargebee.streams.base.os")
    @patch(
        "tap_chargebee.streams.base.open",
        new_callable=mock_open,
        read_data='{"key": "value"}',
    )
    def test_load_shared_schema_refs_with_item_model(self, mock_open, mock_os):
        """
        Test load_shared_schema_refs method with item_model set to False
        """
        self.base_instance.config["item_model"] = False
        self.base_instance.load_shared_schema_ref = MagicMock(
            return_value={"schema1.json": {"key": "value"}}
        )

        result = self.base_instance.load_shared_schema_refs()

        self.assertEqual(self.base_instance.load_shared_schema_ref.call_count, 2)
        self.base_instance.load_shared_schema_ref.assert_any_call("common")
        self.base_instance.load_shared_schema_ref.assert_any_call("plan_model")
        self.assertEqual(
            result, {"schema1.json": {"key": "value"}, "schema1.json": {"key": "value"}}
        )
