import tap_chargebee.client as _client
import unittest
import requests
from unittest import mock


# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"access_token": "test", "expires_in":100}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response


@mock.patch("requests.request", side_effect=get_mock_http_response)
class TestRequestTimeoutValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_request):
        """
        Verify that if request_timeout is not provided in config then default value is used
        """
        config = {"start_date": "2017-01-01T00:00:00Z"}  # No request_timeout in config
        chargebee_client = _client.ChargebeeClient(config)

        # Verify requests.request is called with expected timeout
        self.assertEqual(chargebee_client.request_timeout, 300)

    def test_integer_request_timeout_in_config(self, mocked_request):
        """
        Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100,
        }  # integer timeout in config
        chargebee_client = _client.ChargebeeClient(config)

        # Verify requests.request is called with expected timeout
        self.assertEqual(chargebee_client.request_timeout, 100.0)

    def test_float_request_timeout_in_config(self, mocked_request):
        """
        Verify that if request_timeout is provided in config(float value) then it should be use
        """
        config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 100.5,
        }  # float timeout in config
        chargebee_client = _client.ChargebeeClient(config)

        # Verify requests.request is called with expected timeout
        self.assertEqual(chargebee_client.request_timeout, 100.5)

    def test_string_request_timeout_in_config(self, mocked_request):
        """
        Verify that if request_timeout is provided in config(string value) then it should be use
        """
        config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "100",
        }  # string format timeout in config
        chargebee_client = _client.ChargebeeClient(config)

        # Verify requests.request is called with expected timeout
        self.assertEqual(chargebee_client.request_timeout, 100.0)

    def test_empty_string_request_timeout_in_config(self, mocked_request):
        """
        Verify that if request_timeout is provided in config with empty string then default value is used
        """
        config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "",
        }  # empty string in config
        chargebee_client = _client.ChargebeeClient(config)

        # Verify requests.request is called with expected timeout
        self.assertEqual(chargebee_client.request_timeout, 300)

    def test_zero_request_timeout_in_config(self, mocked_request):
        """
        Verify that if request_timeout is provided in config with zero value then default value is used
        """
        config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": 0.0,
        }  # zero value in config
        chargebee_client = _client.ChargebeeClient(config)

        # Verify requests.request is called with expected timeout
        self.assertEqual(chargebee_client.request_timeout, 300)

    def test_zero_string_request_timeout_in_config(self, mocked_request):
        """
        Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        config = {
            "start_date": "2017-01-01T00:00:00Z",
            "request_timeout": "0.0",
        }  # zero value in config
        chargebee_client = _client.ChargebeeClient(config)

        # Verify requests.request is called with expected timeout
        self.assertEqual(chargebee_client.request_timeout, 300)
