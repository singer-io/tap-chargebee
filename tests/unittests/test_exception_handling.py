from tap_chargebee import client
import unittest
import requests
from unittest import mock


def get_mock_http_response(status_code, contents):
    """Returns mock rep"""
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response


@mock.patch("time.sleep")
@mock.patch("requests.request")
class TestErrorHandling(unittest.TestCase):
    """
    Test cases to verify if the errors are handled as expected while communicating with Chargebee Environment
    """

    config = {"start_date": "2017-01-01T00:00:00Z"}
    chargebee_client = client.ChargebeeClient(config)

    def test_400_Error_response_message(self, mocked_400_successful, mocked_sleep):
        """
        Exception with response message should be raised if 400 status code returned from API
        """
        resp_str = '{"message": "Sorry, Bad Request Error"}'
        mocked_400_successful.return_value = get_mock_http_response(400, resp_str)

        expected_message = "HTTP-error-code: 400, Error: Sorry, Bad Request Error"

        with self.assertRaises(client.ChargebeeBadRequestError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_400_successful.call_count, 5)

    def test_401_Error_response_message(self, mocked_401_successful, mocked_sleep):
        """
        Exception with response message should be raised if 401 status code returned from API
        """
        resp_str = '{"message": "Sorry, authentication failed. Invalid api key"}'
        mocked_401_successful.return_value = get_mock_http_response(401, resp_str)

        expected_message = (
            "HTTP-error-code: 401, Error: Sorry, authentication failed. Invalid api key"
        )

        with self.assertRaises(client.ChargebeeAuthenticationError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_401_successful.call_count, 5)

    def test_403_Error_response_message(self, mocked_403_successful, mocked_sleep):
        """
        Exception with response message should be raised if 403 status code returned from API
        """
        resp_str = '{"message": "Sorry, Operation not permitted"}'
        mocked_403_successful.return_value = get_mock_http_response(403, resp_str)

        expected_message = "HTTP-error-code: 403, Error: Sorry, Operation not permitted"

        with self.assertRaises(client.ChargebeeForbiddenError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_403_successful.call_count, 5)

    def test_404_Error_response_message(self, mocked_404_successful, mocked_sleep):
        """
        Exception with response message should be raised if 404 status code returned from API
        """
        resp_str = '{"message": "Sorry, Resource not found"}'
        mocked_404_successful.return_value = get_mock_http_response(404, resp_str)

        expected_message = "HTTP-error-code: 404, Error: Sorry, Resource not found"

        with self.assertRaises(client.ChargebeeNotFoundError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_404_successful.call_count, 5)

    def test_405_Error_response_message(self, mocked_405_successful, mocked_sleep):
        """
        Exception with response message should be raised if 405 status code returned from API
        """
        resp_str = '{"message": "Sorry, HTTP action not allowed for the API"}'
        mocked_405_successful.return_value = get_mock_http_response(405, resp_str)

        expected_message = (
            "HTTP-error-code: 405, Error: Sorry, HTTP action not allowed for the API"
        )

        with self.assertRaises(client.ChargebeeMethodNotAllowedError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_405_successful.call_count, 5)

    def test_409_Error_response_message(self, mocked_409_successful, mocked_sleep):
        """
        Exception with response message should be raised if 409 status code returned from API
        """
        resp_str = '{"message": "Sorry, The request could not be processed"}'
        mocked_409_successful.return_value = get_mock_http_response(409, resp_str)

        expected_message = (
            "HTTP-error-code: 409, Error: Sorry, The request could not be processed"
        )

        with self.assertRaises(client.ChargebeeNotProcessedError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_409_successful.call_count, 5)

    def test_429_Error_response_message(self, mocked_429_successful, mocked_sleep):
        """
        Exception with response message should be raised if 429 status code returned from API
        """
        resp_str = '{"message": "Sorry, Requesting too many requests"}'
        mocked_429_successful.return_value = get_mock_http_response(429, resp_str)

        expected_message = (
            "HTTP-error-code: 429, Error: Sorry, Requesting too many requests"
        )

        with self.assertRaises(client.ChargebeeRateLimitError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_429_successful.call_count, 5)

    def test_500_Error_response_message(self, mocked_500_successful, mocked_sleep):
        """
        Exception with response message should be raised if 500 status code returned from API
        """
        resp_str = '{"message": "Sorry, Internal server error."}'
        mocked_500_successful.return_value = get_mock_http_response(500, resp_str)

        expected_message = "HTTP-error-code: 500, Error: Sorry, Internal server error."

        with self.assertRaises(client.ChargebeeInternalServiceError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_500_successful.call_count, 5)

    def test_503_Error_response_message(self, mocked_503_successful, mocked_sleep):
        """
        Exception with response message should be raised if 503 status code returned from API
        """
        resp_str = '{"message": "Sorry, Temporary internal server error "}'
        mocked_503_successful.return_value = get_mock_http_response(503, resp_str)

        expected_message = (
            "HTTP-error-code: 503, Error: Sorry, Temporary internal server error "
        )

        with self.assertRaises(client.ChargebeeServiceUnavailableError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), expected_message)
        self.assertEqual(mocked_503_successful.call_count, 5)

    def test_400_error_custom_message(self, mocked_400_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 400 status code returned from API and 'message' not present in response
        """
        mocked_400_successful.return_value = get_mock_http_response(400, "{}")

        expected_message = "HTTP-error-code: 400, Error: The request URI does not match the APIs in the system."

        with self.assertRaises(client.ChargebeeBadRequestError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_400_successful.call_count, 5)

    def test_401_error_custom_message(self, mocked_401_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 401 status code returned from API and 'message' not present in response
        """
        mocked_401_successful.return_value = get_mock_http_response(401, "{}")

        expected_message = (
            "HTTP-error-code: 401, Error: The user is not authenticated to use the API."
        )

        with self.assertRaises(client.ChargebeeAuthenticationError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_401_successful.call_count, 5)

    def test_403_error_custom_message(self, mocked_403_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 403 status code returned from API and 'message' not present in response
        """
        mocked_403_successful.return_value = get_mock_http_response(403, "{}")

        expected_message = "HTTP-error-code: 403, Error: The requested operation is not permitted for the user."

        with self.assertRaises(client.ChargebeeForbiddenError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_403_successful.call_count, 5)

    def test_404_error_custom_message(self, mocked_404_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 404 status code returned from API and 'message' not present in response
        """
        mocked_404_successful.return_value = get_mock_http_response(404, "{}")

        expected_message = (
            "HTTP-error-code: 404, Error: The requested resource was not found."
        )

        with self.assertRaises(client.ChargebeeNotFoundError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_404_successful.call_count, 5)

    def test_405_error_custom_message(self, mocked_405_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 405 status code returned from API and 'message' not present in response
        """
        mocked_405_successful.return_value = get_mock_http_response(405, "{}")

        expected_message = "HTTP-error-code: 405, Error: The HTTP action is not allowed for the requested REST API."

        with self.assertRaises(client.ChargebeeMethodNotAllowedError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_405_successful.call_count, 5)

    def test_409_error_custom_message(self, mocked_409_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 409 status code returned from API and 'message' not present in response
        """
        mocked_409_successful.return_value = get_mock_http_response(409, "{}")

        expected_message = "HTTP-error-code: 409, Error: The request could not be processed because of conflict in the request."

        with self.assertRaises(client.ChargebeeNotProcessedError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_409_successful.call_count, 5)

    def test_429_error_custom_message(self, mocked_429_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 429 status code returned from API and 'message' not present in response
        """
        mocked_429_successful.return_value = get_mock_http_response(429, "{}")

        expected_message = (
            "HTTP-error-code: 429, Error: You are requesting to many requests."
        )

        with self.assertRaises(client.ChargebeeRateLimitError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_429_successful.call_count, 5)

    def test_500_error_custom_message(self, mocked_500_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 500 status code returned from API and 'message' not present in response
        """
        mocked_500_successful.return_value = get_mock_http_response(500, "{}")

        expected_message = "HTTP-error-code: 500, Error: The request could not be processed due to internal server error."

        with self.assertRaises(client.ChargebeeInternalServiceError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_500_successful.call_count, 5)

    def test_503_error_custom_message(self, mocked_503_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 503 status code returned from API and 'message' not present in response
        """
        mocked_503_successful.return_value = get_mock_http_response(503, "{}")

        expected_message = "HTTP-error-code: 503, Error: The request could not be processed due to temporary internal server error."

        with self.assertRaises(client.ChargebeeServiceUnavailableError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_503_successful.call_count, 5)

    def test_5XX_error_custom_message(self, mocked_5xx_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 5XX status code returned from API and 'message' not present in response
        """
        mocked_5xx_successful.return_value = get_mock_http_response(502, "{}")

        expected_message = "HTTP-error-code: 502, Error: Unknown Error"

        with self.assertRaises(client.Server5xxError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_5xx_successful.call_count, 5)

    def test_4XX_error_custom_message(self, mocked_4xx_successful, mocked_sleep):
        """
        Exception with custom message should be raised if 4XX status code returned from API and 'message' not present in response
        """
        mocked_4xx_successful.return_value = get_mock_http_response(450, "{}")

        expected_message = "HTTP-error-code: 450, Error: Unknown Error"

        with self.assertRaises(client.Server4xxError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_4xx_successful.call_count, 5)

    def test_unknown_error_custom_message(
        self, mocked_unknown_successful, mocked_sleep
    ):
        """
        Exception with custom message should be raised if other than 4xx/5xx status code returned from API and 'message' not present in response
        """
        mocked_unknown_successful.return_value = get_mock_http_response(350, "{}")

        expected_message = "HTTP-error-code: 350, Error: Unknown Error"

        with self.assertRaises(client.ChargebeeError) as e:
            self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(str(e.exception), str(expected_message))
        self.assertEqual(mocked_unknown_successful.call_count, 1)

    @mock.patch("tap_chargebee.client.LOGGER", side_effect=requests.exceptions.Timeout)
    def test_200_json_error_message(
        self, mocked_warning, mocked_json_error, mocked_sleep
    ):
        """
        Verify that if response is not in JSON format then warning message is logged and empty dictionary is returned
        """
        mocked_json_error.return_value = get_mock_http_response(200, "ssad")

        response = self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(response, {})
        mocked_warning.warning.assert_called_once_with("Response is not in JSON format")

    def test_200_success_response(self, mocked_json_error, mocked_sleep):
        """
        Verify that if response is not in JSON format then warning message is logged and empty dictionary is returned
        """
        mocked_json_error.return_value = get_mock_http_response(200, '{"data": []}')

        response = self.chargebee_client.make_request("/abc", "GET")

        self.assertEqual(response, {"data": []})


@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):

    @mock.patch("requests.request", side_effect=requests.exceptions.Timeout)
    def test_request_timeout_backoff(self, mocked_request, mocked_sleep):
        """
        Verify make_request function is backoff for 5 times on Timeout exception
        """
        config = {"start_date": "2017-01-01T00:00:00Z"}

        chargebee_client = client.ChargebeeClient(config)
        with self.assertRaises(requests.exceptions.Timeout) as e:
            chargebee_client.make_request("/abc", "GET")

        # Verify that requests.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)


@mock.patch("time.sleep")
class TestConnectionErrorBackoff(unittest.TestCase):

    @mock.patch("requests.request", side_effect=requests.exceptions.ConnectionError)
    def test_request_timeout_backoff(self, mocked_request, mocked_sleep):
        """
        Verify make_request function is backoff for 5 times on ConnectionError exception
        """
        config = {"start_date": "2017-01-01T00:00:00Z"}
        chargebee_client = client.ChargebeeClient(config)

        with self.assertRaises(requests.exceptions.ConnectionError) as e:
            chargebee_client.make_request("/abc", "GET")

        # Verify that requests.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)
