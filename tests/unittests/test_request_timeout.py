import unittest
from unittest.mock import patch

from requests.exceptions import Timeout, ConnectionError
import tap_typeform.http as client_

REQUEST_TIMEOUT_INT = 300
REQUEST_TIMEOUT_STR = "300"
REQUEST_TIMEOUT_FLOAT = 300.05

@patch('time.sleep')
class TestRequestTimeouts(unittest.TestCase):
    
    endpoint = "forms"

    @patch('requests.Request', side_effect=Timeout)
    def test_request_timeout_backoff_for_string_value_in_config(self, mocked_request, mock_sleep):
        """
        We mock request.Request method to raise a `Timeout` and expect the tap to retry this up to 
        5 times when stiring "300" value of `request_timeout` passed
        """
        config = {'token': '123', "request_timeout": REQUEST_TIMEOUT_STR}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except Timeout as e:
            pass

        # Verify that request.Request called 5 times
        self.assertEqual(mocked_request.call_count, 5)
    
    @patch('requests.Request', side_effect=Timeout)
    def test_request_timeout_backoff_for_int_value_in_config(self, mocked_request, mock_sleep):
        """
        We mock request.Request method to raise a `Timeout` and expect the tap to retry this up to 
        5 times when int 300 value of `request_timeout` passed
        """
        config = {'token': '123', "request_timeout": REQUEST_TIMEOUT_INT}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except Timeout as e:
            pass

        # Verify that request.Request called 5 times
        self.assertEqual(mocked_request.call_count, 5)
        
    @patch('requests.Request', side_effect=Timeout)
    def test_request_timeout_backoff_for_float_value_in_config(self, mocked_request, mock_sleep):
        """
        We mock request.Request method to raise a `Timeout` and expect the tap to retry this up to 
        5 times when float 300.05 value of `request_timeout` passed
        """
        config = {'token': '123', "request_timeout": REQUEST_TIMEOUT_FLOAT}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except Timeout as e:
            pass

        # Verify that request.Request called 5 times
        self.assertEqual(mocked_request.call_count, 5)

    @patch('requests.Request', side_effect=Timeout)
    def test_request_timeout_backoff_for_empty_string_value_in_config(self, mocked_request, mock_sleep):
        """
        We mock request.Request method to raise a `Timeout` and expect the tap to retry this up to 
        5 times when empty string value of `request_timeout` passed
        """
        config = {'token': '123', "request_timeout": ""}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except Timeout as e:
            pass

        # Verify that request.Request called 5 times
        self.assertEqual(mocked_request.call_count, 5)
        
    @patch('requests.Request', side_effect=Timeout)
    def test_request_timeout_backoff_for_zero_string_value_in_config(self, mocked_request, mock_sleep):
        """
        We mock request.Request method to raise a `Timeout` and expect the tap to retry this up to 
        5 times when zero string value of `request_timeout` passed
        """
        config = {'token': '123', "request_timeout": "0"}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except Timeout as e:
            pass

        # Verify that request.Request called 5 times
        self.assertEqual(mocked_request.call_count, 5)
        
    @patch('requests.Request', side_effect=Timeout)
    def test_request_timeout_backoff_for_zero_int_value_in_config(self, mocked_request, mock_sleep):
        """
        We mock request.Request method to raise a `Timeout` and expect the tap to retry this up to 
        5 times when int 0 value of `request_timeout` passed
        """        
        config = {'token': '123', "request_timeout": 0}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except Timeout as e:
            pass

        # Verify that request.Request called 5 times
        self.assertEqual(mocked_request.call_count, 5)
        
    @patch('requests.Request', side_effect=Timeout)
    def test_request_timeout_backoff_for_none_value_in_config(self, mocked_request, mock_sleep):
        """
        We mock request.Request method to raise a `Timeout` and expect the tap to retry this up to 
        5 times when `request_timeout` does not passed
        """        
        config = {'token': '123', "request_timeout": 0}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except Timeout as e:
            pass

        # Verify that request.Request called 5 times
        self.assertEqual(mocked_request.call_count, 5)

@patch('time.sleep')
class TestConnectionError(unittest.TestCase):
    
    endpoint = "forms"

    @patch('requests.Request', side_effect=ConnectionError)
    def test_connection_error_backoff(self, mocked_request, mock_sleep):
        """
        We mock request.Request method to raise a `ConnectionError` and expect the tap to retry this up to 5 times
        """
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except ConnectionError as e:
            pass

        # Verify that request.Request called 5 times
        self.assertEqual(mocked_request.call_count, 5)
 