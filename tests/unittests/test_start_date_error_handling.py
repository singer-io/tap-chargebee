from tap_chargebee.streams.base import BaseChargebeeStream
import unittest
import singer
from unittest import mock

class TestStartDateErrorHandling(unittest.TestCase):
    """
    Test cases to verify is a start date giving proper error message for wrong format of start date 
    """

    def mock_get_config_start_date(state,table,msg):
        return "2015-06-08"

    @mock.patch('tap_chargebee.state.get_last_record_value_for_table')
    @mock.patch('tap_framework.config.get_config_start_date',side_effect=mock_get_config_start_date)
    def test_sync_data_for_wrong_format_start_date(self, mock_get_config_start_date,mock_get_last_record_value_for_table):
        """
        Test cases to verify is a start date giving proper error message for wrong format of start date
        """
        base = BaseChargebeeStream({'start_date':'2015-06-08'},{},None,None)
        base.ENTITY = None
        try:
            base.sync_data()
        except ValueError as e:
            expected_message = "start_date must be in '%Y-%m-%dT%H:%M:%SZ' format"
            # Verifying the message should be API response
            self.assertEquals(str(e), str(expected_message))