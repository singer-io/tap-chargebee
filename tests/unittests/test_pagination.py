import unittest
from unittest.mock import patch
from tap_chargebee.client import ChargebeeClient


class TestClientPagination(unittest.TestCase):

    config = {"start_date": "2017-01-01T00:00:00Z", "include_deleted": "false"}
    chargebee_client = ChargebeeClient(config)

    @patch("tap_chargebee.client.ChargebeeClient.make_request")
    def test_get_offset_based_pages_no_pages(self, mock_make_request):
        """
        Test get_offset_based_pages method with no pages
        """
        # Mock the response to simulate no pages
        mock_make_request.return_value = {"list": [], "next_offset": None}

        pages = list(
            self.chargebee_client.get_offset_based_pages(
                "http://example.com", "GET", None
            )
        )

        self.assertEqual(pages, [[]])
        mock_make_request.assert_called_once()

    @patch("tap_chargebee.client.ChargebeeClient.make_request")
    def test_get_offset_based_pages_multiple_pages(self, mock_make_request):
        """
        Test get_offset_based_pages method with multiple pages
        """
        # Mock the response to simulate multiple pages
        mock_make_request.side_effect = [
            {"list": [1, 2, 3], "next_offset": "offset_1"},
            {"list": [4, 5, 6], "next_offset": None},
        ]

        pages = list(
            self.chargebee_client.get_offset_based_pages(
                "http://example.com", "GET", None
            )
        )

        self.assertEqual(pages, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(mock_make_request.call_count, 2)

    @patch("tap_chargebee.client.ChargebeeClient.make_request")
    def test_get_offset_based_pages_with_sort_by(self, mock_make_request):
        """
        Test get_offset_based_pages method with sort_by parameter
        """
        # Mock the response to simulate sorted pages
        mock_make_request.return_value = {"list": [1, 2, 3], "next_offset": None}

        pages = list(
            self.chargebee_client.get_offset_based_pages(
                "http://example.com",
                "GET",
                "created_at",
                params={"updated_at[after]": 24234234},
            )
        )

        self.assertEqual(pages, [[1, 2, 3]])
        mock_make_request.assert_called_once_with(
            "http://example.com",
            "GET",
            {
                "limit": 100,
                "include_deleted": False,
                "sort_by[asc]": "created_at",
                "updated_at[after]": 24234234,
            },
            None,
        )
