import tap_chargebee
import unittest
import singer
from unittest import mock

class Namespace:

    def __init__(self,catalog,config,discover,properties,state):
        self.catalog = catalog
        self.config = config
        self.discover = discover
        self.properties = properties
        self.state = state

class TestStartDateErrorHandling(unittest.TestCase):
    """
    Test cases to verify is a start date giving proper error message for wrong format of start date 
    """

    def mock_parse_args(required_config_keys):
        
        return Namespace(catalog=None, config={'start_date': '2019-06-24', 'api_key': 'test_111111111111111111111111111111111111', 'site': 'test-test', 'include_deleted': True}, discover=False, properties=None, state={})

    @mock.patch('tap_chargebee.client.ChargebeeClient')
    @mock.patch('singer.utils.parse_args',side_effect=mock_parse_args)
    @mock.patch('tap_chargebee.get_available_streams')
    def test_sync_data_for_wrong_format_start_date(self, mock_get_available_streams, mock_parse_args, mock_ChargebeeClient):
        """
        Test cases to verify is a start date giving proper error message for wrong format of start date
        """
        try:
            tap_chargebee.main()
        except ValueError as e:
            expected_message = "start_date must be in 'YYYY-mm-ddTHH:MM:SSZ' format"
            # Verifying the message should be API response
            self.assertEquals(str(e), str(expected_message))