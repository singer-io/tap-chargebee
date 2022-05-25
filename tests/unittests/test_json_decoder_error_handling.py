import tap_chargebee.client as _client
import unittest
import requests
import json
from unittest import mock

def get_mock_http_response(status_code, contents):
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response

@mock.patch('requests.request')
class TestJSONDecoderHandling(unittest.TestCase):
    """
    Test cases to verify if the json decoder error is handled as expected while communicating with Chargebee Environment 
    """

    @mock.patch("time.sleep")
    def test_json_decode_successfull_4XX(self, mocked_sleep, mocked_jsondecode_successful):
        """
        Exception with response message should be raised if valid JSON response returned with 4XX error
        """
        json_decode_str = {
            "message": "Sorry, authentication failed. Invalid api key",
            "api_error_code": "api_authentication_failed",
            "error_code": "api_authentication_invalid_key",
            "error_msg": "Sorry, authentication failed. Invalid api key",
            "http_status_code": 401
        }
        mocked_jsondecode_successful.return_value = get_mock_http_response(
            401, json.dumps(json_decode_str))

        config = {"start_date": "2017-01-01T00:00:00Z"}
        chargebee_client = _client.ChargebeeClient(config)

        expected_message = "HTTP-error-code: 401, Error: Sorry, authentication failed. Invalid api key"
        with self.assertRaises(_client.ChargebeeAuthenticationError) as e:
            chargebee_client.make_request("/abc", "GET")
 
            # Verifying the message should be API response
            self.assertEquals(str(e), str(expected_message))
    
    @mock.patch("time.sleep")
    def test_json_decode_failed_4XX(self, mocked_sleep, mocked_jsondecode_failure):
        """
        Exception with proper custom error message should be raised if invalid JSON response returned with 4XX error
        """
        json_decode_error_str = '<>"success": true, "data" : []}'
        mocked_jsondecode_failure.return_value = get_mock_http_response(
            400, json_decode_error_str)

        config = {"start_date": "2017-01-01T00:00:00Z"}
        chargebee_client = _client.ChargebeeClient(config)

        expected_message = "HTTP-error-code: 400, Error: The request URI does not match the APIs in the system."
        with self.assertRaises(_client.ChargebeeBadRequestError) as e:
            chargebee_client.make_request("/abc", "GET")

            # Verifying the formatted message for json decoder exception
            self.assertEquals(str(e), str(expected_message))

    def test_json_decode_200(self, mocked_jsondecode_successful):
        """
        No exception should be raised for successfull API request
        """
        json_decode_str = '{"success": true, "data" : []}'
        mocked_jsondecode_successful.return_value = get_mock_http_response(
            200, json_decode_str)

        config = {"start_date": "2017-01-01T00:00:00Z"}
        chargebee_client = _client.ChargebeeClient(config)

        # No exception should be raised with JSON decoder error
        chargebee_client.make_request('/abc', 'GET')

        self.assertEqual(mocked_jsondecode_successful.call_count, 1)