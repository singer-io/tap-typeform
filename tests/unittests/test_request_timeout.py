import json
import unittest
from unittest.mock import patch
from parameterized import parameterized

import requests
import tap_typeform.client as client_

REQUEST_TIMEOUT_INT = 300
REQUEST_TIMEOUT_STR = "300"
REQUEST_TIMEOUT_FLOAT = 300.0


test_config_path = "/tmp/test_config.json"

def write_new_config_file(**kwargs):
    test_config = {}
    with open(test_config_path, 'w') as config:
        for key, value in kwargs.items():
            test_config[key] = value
        config.write(json.dumps(test_config))


# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"accounts":[{"id": 12}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response

@patch('time.sleep')
@patch('requests.Session.get', side_effect = get_mock_http_response)
@patch('requests.Request.prepare')
class TestRequestTimeoutsValue(unittest.TestCase):

    endpoint = "forms"

    @parameterized.expand([
        (REQUEST_TIMEOUT_STR, REQUEST_TIMEOUT_FLOAT),
        (REQUEST_TIMEOUT_INT, REQUEST_TIMEOUT_FLOAT),
        (REQUEST_TIMEOUT_FLOAT, REQUEST_TIMEOUT_FLOAT),
        ("", REQUEST_TIMEOUT_INT),
        ("0", REQUEST_TIMEOUT_INT),
        (0, REQUEST_TIMEOUT_INT),
    ])
    def test_request_timeout_for_diff_values(self, mocked_request, mock_get, mock_sleep, time_out_value, expected_value):
        """
        Test the various values of timeout:
            - For string, integer, float type of values, converts to float
            - For null string, zero(string), zero(integer), takes default integer value
        """
        test_config = {"token": "old_access_token",
                       "request_timeout": time_out_value}
        write_new_config_file(**test_config)
        client = client_.Client(test_config, test_config_path, False)
        url = client.build_url(self.endpoint)

        # Call request method which calls `requests.Session.get` with timeout
        client.request(url)

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mock_get.call_args
        self.assertEqual(kwargs.get('timeout'), expected_value) # Verify timeout argument

    def test_no_request_timeout_value_in_config(self, mocked_request, mock_send, mock_sleep):
        """
        Verify that if request_timeout is not provided in the config then the default value is used
        """
        test_config = {"token": "old_access_token"}
        write_new_config_file(**test_config)
        client = client_.Client(test_config, test_config_path, False)
        url = client.build_url(self.endpoint)
        # Call request method which calls `requests.Session.get` with timeout
        client.request(url)

        # Verify requests.Session.get is called with expected timeout
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument
