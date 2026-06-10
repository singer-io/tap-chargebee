import unittest
from unittest.mock import patch, MagicMock, mock_open
from tap_chargebee.streams.comments import CommentsStream
from tap_chargebee.streams.customers import CustomersStream
from tap_chargebee import _apply_access_checks
from tap_chargebee.client import ChargebeeClient, ChargebeeForbiddenError


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


class TestApplyAccessChecks(unittest.TestCase):

    def setUp(self):
        self.config = {
            "start_date": "2017-01-01T00:00:00Z",
            "include_deleted": "false",
            "site": "test-site",
            "api_key": "test-key",
            "item_model": False,
        }
        self.mock_client = MagicMock(spec=ChargebeeClient)

    def test_all_accessible_returns_all_streams(self):
        self.mock_client.check_access.return_value = True
        result = _apply_access_checks(self.config, {}, self.mock_client, [CommentsStream, CustomersStream])
        self.assertEqual(result, [CommentsStream, CustomersStream])

    def test_forbidden_stream_is_excluded(self):
        # Simulate comments endpoint being forbidden/inaccessible
        self.mock_client.check_access.side_effect = lambda url, method: "comments" not in url
        result = _apply_access_checks(self.config, {}, self.mock_client, [CommentsStream, CustomersStream])
        self.assertEqual(result, [CustomersStream])

    def test_all_inaccessible_raises_forbidden_error(self):
        self.mock_client.check_access.return_value = False
        with self.assertRaises(ChargebeeForbiddenError):
            _apply_access_checks(self.config, {}, self.mock_client, [CommentsStream, CustomersStream])

    @patch("tap_chargebee.LOGGER")
    def test_warning_logged_for_excluded_stream(self, mock_logger):
        # Simulate comments endpoint being forbidden/inaccessible
        self.mock_client.check_access.side_effect = lambda url, method: "comments" not in url
        _apply_access_checks(self.config, {}, self.mock_client, [CommentsStream, CustomersStream])
        mock_logger.warning.assert_called_once()
        self.assertEqual("comments", mock_logger.warning.call_args[0][1])
