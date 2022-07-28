import unittest
from unittest.mock import patch
from parameterized import parameterized

import requests
import tap_typeform.client as client_

PAGE_SIZE_INT = 100
PAGE_SIZE_STR = "100"
PAGE_SIZE_FLOAT = 100.0
PAGE_SIZE_ZERO = 0
PAGE_SIZE_STR_ZERO = "0"
PAGE_SIZE_INVALID_STRING = 'abc'
PAGE_SIZE_DEFAULT_FORMS = 200
PAGE_SIZE_DEFAULT = 1000

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
class TestPageSizeValue(unittest.TestCase):

    @parameterized.expand([
        (PAGE_SIZE_INT, PAGE_SIZE_INT),
        (PAGE_SIZE_STR, PAGE_SIZE_INT),
        (PAGE_SIZE_FLOAT, PAGE_SIZE_INT),
        (PAGE_SIZE_INVALID_STRING, PAGE_SIZE_DEFAULT_FORMS),
        (PAGE_SIZE_INVALID_STRING, PAGE_SIZE_DEFAULT_FORMS),
        (PAGE_SIZE_ZERO, PAGE_SIZE_DEFAULT_FORMS),
        (PAGE_SIZE_STR_ZERO, PAGE_SIZE_DEFAULT_FORMS),
    ])
    def test_page_size_for_diff_values_forms(self, mocked_request, mock_get, mock_sleep, page_size_value, expected_value):
        """
        Test the various values of page_size:
            - For string, integer, float type of values, converts to float
            - For null string, zero(string), zero(integer), takes default integer value
        """
        endpoint = "forms"
        config = {'token': '123', "page_size": page_size_value}
        client = client_.Client(config)
        client.get_page_size(config)
        url = client.build_url(self.endpoint)
        
        # Call request method which calls `requests.Session.get` with page_size
        client.request(url)

        # Verify the form_page_size is the same as the expected value
        self.assertEqual(client.form_page_size, expected_value)

    @parameterized.expand([
        (PAGE_SIZE_INT, PAGE_SIZE_INT),
        (PAGE_SIZE_STR, PAGE_SIZE_INT),
        (PAGE_SIZE_FLOAT, PAGE_SIZE_INT),
        (PAGE_SIZE_INVALID_STRING, PAGE_SIZE_DEFAULT),
        (PAGE_SIZE_INVALID_STRING, PAGE_SIZE_DEFAULT),
        (PAGE_SIZE_ZERO, PAGE_SIZE_DEFAULT),
        (PAGE_SIZE_STR_ZERO, PAGE_SIZE_DEFAULT),
    ])
    def test_page_size_for_diff_values(self, mocked_request, mock_get, mock_sleep, page_size_value, expected_value):
        """
        Test the various values of page_size:
            - For string, integer, float type of values, converts to float
            - For null string, zero(string), zero(integer), takes default integer value
        """
        endpoint = "landings"
        config = {'token': '123', "page_size": page_size_value}
        client = client_.Client(config)
        client.get_page_size(config)
        url = client.build_url(self.endpoint)
        
        # Call request method which calls `requests.Session.get` with page_size
        client.request(url)

        # Verify the page_size is the same as the expected value
        self.assertEqual(client.page_size, expected_value)
