import backoff
import time
import requests
import singer
import json

from simplejson.scanner import JSONDecodeError
from singer import utils
from tap_framework.client import BaseClient


LOGGER = singer.get_logger()


class Server4xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class ChargebeeClient(BaseClient):

    def __init__(self, config, api_result_limit=100):
        super().__init__(config)

        self.api_result_limit = api_result_limit
        self.include_deleted = self.config.get('include_deleted', True)
        self.user_agent = self.config.get('user_agent')

    def get_headers(self):
        headers = {}

        if self.config.get('user_agent'):
            headers['User-Agent'] = self.config.get('user_agent')

        return headers

    def get_params(self, params):

        if params is None:
            params = {}

        params['limit'] = self.api_result_limit
        params['include_deleted'] = self.include_deleted

        return params

    @backoff.on_exception(backoff.expo,
                          (Server4xxError, Server429Error, JSONDecodeError),
                          max_tries=6,
                          factor=2)
    @utils.ratelimit(100, 60)
    def make_request(self, url, method, params=None, body=None):

        if params is None:
            params = {}

        LOGGER.info("Making {} request to {}".format(method, url))

        response = requests.request(
            method,
            url,
            auth=(self.config.get("api_key"), ''),
            headers=self.get_headers(),
            params=self.get_params(params),
            json=body)

        if response.status_code == 429:
            sleep_time = response.headers.get("Retry-After", 60)
            time.sleep(int(sleep_time))
            raise Server429Error()

        if response.status_code >= 400:
            raise Server4xxError(response.text)

        response_json = response.json()

        return response_json
