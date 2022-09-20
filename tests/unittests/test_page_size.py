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

class TestPageSizeValue(unittest.TestCase):

    endpoint = "forms"
    @parameterized.expand([
        (PAGE_SIZE_INT, PAGE_SIZE_INT),
        (PAGE_SIZE_STR, PAGE_SIZE_INT),
        (PAGE_SIZE_FLOAT, PAGE_SIZE_INT),
    ])
    def test_page_size_for_diff_values_forms(self, page_size_value, expected_value):
        """
        Test the various values of page_size:
            - For string, integer, float type of values, converts to float
            - For null string, zero(string), zero(integer), takes default integer value
        """
        self.endpoint = "forms"
        config = {'token': '123', "page_size": page_size_value}
        client = client_.Client(config)
        client.get_page_size(config)

        # Verify the form_page_size is the same as the expected value
        self.assertEqual(client.form_page_size, expected_value)

    @parameterized.expand([
        (PAGE_SIZE_INT, PAGE_SIZE_INT),
        (PAGE_SIZE_STR, PAGE_SIZE_INT),
        (PAGE_SIZE_FLOAT, PAGE_SIZE_INT),
    ])
    def test_page_size_for_diff_values(self, page_size_value, expected_value):
        """
        Test the various values of page_size:
            - For string, integer, float type of values, converts to float
            - For null string, zero(string), zero(integer), takes default integer value
        """
        self.endpoint = "landings"
        config = {'token': '123', "page_size": page_size_value}
        client = client_.Client(config)
        client.get_page_size(config)

        # Verify the page_size is the same as the expected value
        self.assertEqual(client.page_size, expected_value)

    @parameterized.expand([
        (PAGE_SIZE_INVALID_STRING, Exception),
        (PAGE_SIZE_STR_ZERO, Exception),
        (PAGE_SIZE_ZERO, Exception),
    ])
    def test_page_size_for_invalid_values(self, page_size_value, error):
        """
        Test the various values of page_size:
            - For string, integer, float type of values, converts to float
            - For null string, zero(string), zero(integer), takes default integer value
        """
        self.endpoint = "landings"
        print(page_size_value)
        config = {'token': '123', "page_size": page_size_value}
        # Verify the tap raises Exception
        with self.assertRaises(error) as e:
            client = client_.Client(config)
            client.get_page_size(config)
        # Verify the tap raises an error with expected error message
        self.assertEqual(str(e.exception), "The entered page size is invalid, it should be a valid integer.")
