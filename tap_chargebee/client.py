import backoff
import requests
from singer import utils, get_logger
from requests.exceptions import Timeout, ConnectionError

LOGGER = get_logger()
LIMIT = 100
REQUEST_TIMEOUT = 300

# Define custom exceptions
class ChargebeeError(Exception):
    pass

class Server4xxError(ChargebeeError):
    pass

class Server5xxError(ChargebeeError):
    pass

class ChargebeeBadRequestError(Server4xxError):
    pass

class ChargebeeAuthenticationError(Server4xxError):
    pass

class ChargebeeForbiddenError(Server4xxError):
    pass

class ChargebeeNotFoundError(Server4xxError):
    pass

class ChargebeeMethodNotAllowedError(Server4xxError):
    pass

class ChargebeeNotProcessedError(Server4xxError):
    pass

class ChargebeeRateLimitError(Server4xxError):
    pass

class ChargebeeInternalServiceError(Server5xxError):
    pass

class ChargebeeServiceUnavailableError(Server5xxError):
    pass


STATUS_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": ChargebeeBadRequestError,
        "message": "The request URI does not match the APIs in the system.",
    },
    401: {
        "raise_exception": ChargebeeAuthenticationError,
        "message": "The user is not authenticated to use the API.",
    },
    403: {
        "raise_exception": ChargebeeForbiddenError,
        "message": "The requested operation is not permitted for the user.",
    },
    404: {
        "raise_exception": ChargebeeNotFoundError,
        "message": "The requested resource was not found.",
    },
    405: {
        "raise_exception": ChargebeeMethodNotAllowedError,
        "message": "The HTTP action is not allowed for the requested REST API.",
    },
    409: {
        "raise_exception": ChargebeeNotProcessedError,
        "message": "The request could not be processed because of conflict in the request.",
    },
    429: {
        "raise_exception": ChargebeeRateLimitError,
        "message": "You are requesting to many requests.",
    },
    500: {
        "raise_exception": ChargebeeInternalServiceError,
        "message": "The request could not be processed due to internal server error.",
    },
    503: {
        "raise_exception": ChargebeeServiceUnavailableError,
        "message": "The request could not be processed due to temporary internal server error.",
    },
}


def get_exception_for_status_code(status_code):
    """Map the input status_code with the corresponding Exception Class \
        using 'STATUS_CODE_EXCEPTION_MAPPING' dictionary."""

    exception = STATUS_CODE_EXCEPTION_MAPPING.get(status_code, {}).get(
        "raise_exception"
    )
    # If exception is not mapped for any code then use Server4xxError and Server5xxError respectively
    if not exception:
        if status_code > 400 and status_code < 500:
            exception = Server4xxError
        elif 500 <= status_code < 600:
            exception = Server5xxError
        else:
            exception = ChargebeeError
    return exception


def raise_for_error(response):
    """Raises error class with appropriate msg for the response"""
    try:
        json_response = response.json()
    except requests.exceptions.JSONDecodeError:
        LOGGER.warning("Response is not in JSON format")
        json_response = {}

    if response.status_code == 200:
        return json_response

    status_code = response.status_code
    msg = json_response.get(
        "message",
        STATUS_CODE_EXCEPTION_MAPPING.get(status_code, {}).get(
            "message", "Unknown Error"
        ),
    )
    message = f"HTTP-error-code: {status_code}, Error: {msg}"
    exc = get_exception_for_status_code(status_code)
    raise exc(message) from None


class ChargebeeClient:

    def __init__(self, config: dict):
        self.config = config
        self.request_timeout = self.get_request_timeout()
        self.include_deleted = self.get_include_deleted()

    def get_headers(self):
        """
        Returns headers for the request
        """
        headers = {}
        if self.config.get("user_agent"):
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_include_deleted(self):
        """
        Returns whether to include deleted records based on config.
        """
        include_deleted = self.config.get("include_deleted")
        return include_deleted not in ["false", "False", False]

    def get_request_timeout(self):
        """
        Set request timeout to config param `request_timeout` value.
        """
        config_request_timeout = self.config.get("request_timeout")
        if config_request_timeout and float(config_request_timeout):
            request_timeout = float(config_request_timeout)
        else:
            # If value is 0,"0","" or not passed then set default to 300 seconds.
            request_timeout = REQUEST_TIMEOUT

        return request_timeout

    @backoff.on_exception(
        backoff.expo,
        (Server4xxError, Server5xxError, Timeout, ConnectionError),
        max_tries=5,
        factor=3,
    )
    @utils.ratelimit(100, 60)
    def make_request(
        self, url: str, method: str, params: dict = None, body: dict = None
    ):
        """
        Make a request to the Chargebee API
        Args:
            url (str): The URL to make the request to.
            method (str): The HTTP method to use.
            params (dict, optional): Additional parameters for the request. Defaults to None.
            body (dict, optional): The body of the request. Defaults to None.

        Returns:
            dict: The JSON response from the API.
        """
        LOGGER.info(f"Making {method} request to {url}")

        try:
            response = requests.request(
                method,
                url,
                auth=(self.config.get("api_key"), ""),
                headers=self.get_headers(),
                params=params,
                json=body,
                timeout=self.request_timeout,
            )

            return raise_for_error(response)
        except requests.exceptions.RequestException as e:
            LOGGER.error("Request failed: %s", str(e))
            raise

    def get_offset_based_pages(
        self,
        url: str,
        method: str,
        sort_by: str,
        params: dict = None,
        body: dict = None,
    ):
        """
        Get all pages by using offset-based pagination.
        Args:
            url (str): The URL to make the request to.
            method (str): The HTTP method to use.
            sort_by (str, optional): The field to sort by.
            params (dict, optional): Additional parameters for the request. Defaults to None.
            body (dict, optional): The body of the request. Defaults to None.

        Yields:
            list: A list of items from the current page.
        """
        params = params or {}
        params.update({"limit": LIMIT, "include_deleted": self.include_deleted})
        if sort_by:
            params["sort_by[asc]"] = sort_by

        # Loop through the pages until next_offset is None
        while True:
            response = self.make_request(url, method, params, body)
            yield response.get("list", [])

            next_offset = response.get("next_offset")
            if not next_offset:
                LOGGER.info("Final offset reached. Ending Pagination.")
                break

            LOGGER.info("Advancing by one offset. %s", next_offset)
            params["offset"] = next_offset
