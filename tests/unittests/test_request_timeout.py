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

@mock.patch('requests.request', side_effect = get_mock_http_response)
class TestRequestTimeoutValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        config = {"start_date": "2017-01-01T00:00:00Z"} # No request_timeout in config
        chargebee_client = _client.ChargebeeClient(config)

        # Call make_request method which call requests.request with timeout
        chargebee_client.make_request('/abc', 'GET')

        # Verify requests.request is called with expected timeout
        mocked_request.assert_called_with('GET', '/abc', auth=(None, ''), headers={}, json=None,
                                          params={'limit': 100, 'include_deleted': True},
                                          timeout=300) # Expected timeout

    def test_integer_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": 100} # integer timeout in config
        chargebee_client = _client.ChargebeeClient(config)

        # Call make_request method which call requests.request with timeout
        chargebee_client.make_request('/abc', 'GET')

        # Verify requests.request is called with expected timeout
        mocked_request.assert_called_with('GET', '/abc', auth=(None, ''), headers={}, json=None,
                                          params={'limit': 100, 'include_deleted': True},
                                          timeout=100.0) # Expected timeout

    def test_float_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": 100.5} # float timeout in config
        chargebee_client = _client.ChargebeeClient(config)

        # Call make_request method which call requests.request with timeout
        chargebee_client.make_request('/abc', 'GET')

        # Verify requests.request is called with expected timeout
        mocked_request.assert_called_with('GET', '/abc', auth=(None, ''), headers={}, json=None,
                                          params={'limit': 100, 'include_deleted': True},
                                          timeout=100.5) # Expected timeout

    def test_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": "100"} # string format timeout in config
        chargebee_client = _client.ChargebeeClient(config)

        # Call make_request method which call requests.request with timeout
        chargebee_client.make_request('/abc', 'GET')

        # Verify requests.request is called with expected timeout
        mocked_request.assert_called_with('GET', '/abc', auth=(None, ''), headers={}, json=None,
                                          params={'limit': 100, 'include_deleted': True},
                                          timeout=100.0) # Expected timeout

    def test_empty_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": ''} # empty string in config
        chargebee_client = _client.ChargebeeClient(config)

        # Call make_request method which call requests.request with timeout
        chargebee_client.make_request('/abc', 'GET')

        # Verify requests.request is called with expected timeout
        mocked_request.assert_called_with('GET', '/abc', auth=(None, ''), headers={}, json=None,
                                          params={'limit': 100, 'include_deleted': True},
                                          timeout=300) # Expected timeout

    def test_zero_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": 0.0} # zero value in config
        chargebee_client = _client.ChargebeeClient(config)

        # Call make_request method which call requests.request with timeout
        chargebee_client.make_request('/abc', 'GET')

        # Verify requests.request is called with expected timeout
        mocked_request.assert_called_with('GET', '/abc', auth=(None, ''), headers={}, json=None,
                                          params={'limit': 100, 'include_deleted': True},
                                          timeout=300) # Expected timeout

    def test_zero_string_request_timeout_in_config(self, mocked_request):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        config = {"start_date": "2017-01-01T00:00:00Z", "request_timeout": '0.0'} # zero value in config
        chargebee_client = _client.ChargebeeClient(config)

        # Call make_request method which call requests.request with timeout
        chargebee_client.make_request('/abc', 'GET')

        # Verify requests.request is called with expected timeout
        mocked_request.assert_called_with('GET', '/abc', auth=(None, ''), headers={}, json=None,
                                          params={'limit': 100, 'include_deleted': True},
                                          timeout=300) # Expected timeout


@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):

    @mock.patch("requests.request", side_effect = requests.exceptions.Timeout)
    def test_request_timeout_backoff(self, mocked_request, mocked_sleep):
        """
            Verify make_request function is backoff for 5 times on Timeout exceeption
        """
        config = {"start_date": "2017-01-01T00:00:00Z"}
        chargebee_client = _client.ChargebeeClient(config)

        try:
            chargebee_client.make_request('/abc', 'GET')
        except requests.exceptions.Timeout:
            pass

        # Verify that requests.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)


@mock.patch("time.sleep")
class TestConnectionErrorBackoff(unittest.TestCase):

    @mock.patch("requests.request", side_effect = requests.exceptions.ConnectionError)
    def test_request_timeout_backoff(self, mocked_request, mocked_sleep):
        """
            Verify make_request function is backoff for 5 times on ConnectionError exceeption
        """
        config = {"start_date": "2017-01-01T00:00:00Z"}
        chargebee_client = _client.ChargebeeClient(config)

        try:
            chargebee_client.make_request('/abc', 'GET')
        except requests.exceptions.ConnectionError:
            pass

        # Verify that requests.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)
