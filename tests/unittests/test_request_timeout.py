import unittest
from unittest.mock import patch

import requests
import tap_typeform.http as client_

REQUEST_TIMEOUT_INT = 300
REQUEST_TIMEOUT_STR = "300"
REQUEST_TIMEOUT_FLOAT = 300.0

# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"accounts":[{"id": 12}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response

@patch('time.sleep')
@patch('requests.Session.send', side_effect = get_mock_http_response)
@patch('requests.Request.prepare')
class TestRequestTimeoutsValue(unittest.TestCase):
    
    endpoint = "forms"
    def test_request_timeout_for_string_value_in_config(self, mocked_request, mock_send, mock_sleep):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        config = {'token': '123', "request_timeout": REQUEST_TIMEOUT_STR}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        
        # Call request method which call requests.Session.request with timeout
        client.request('GET', url)

        # Verify requests.Session.send is called with expected timeout
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

    
    def test_request_timeout_for_int_value_in_config(self, mocked_request, mock_send, mock_sleep):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        config = {'token': '123', "request_timeout": REQUEST_TIMEOUT_INT}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        # Call request method which call requests.Session.send with timeout
        client.request('GET', url)

        # Verify requests.Session.send is called with expected timeout
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument
        
    def test_request_timeout_for_float_value_in_config(self, mocked_request, mock_send, mock_sleep):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        config = {'token': '123', "request_timeout": REQUEST_TIMEOUT_FLOAT}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        # Call request method which call requests.Session.send with timeout
        client.request('GET', url)

        # Verify requests.Session.send is called with expected timeout
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_FLOAT) # Verify timeout argument

    def test_request_timeout_for_empty_string_value_in_config(self, mocked_request, mock_send, mock_sleep):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        config = {'token': '123', "request_timeout": ""}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        # Call request method which call requests.Session.send with timeout
        client.request('GET', url)

        # Verify requests.Session.send is called with expected timeout
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument
        
    def test_request_timeout_for_zero_string_value_in_config(self, mocked_request, mock_send, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        config = {'token': '123', "request_timeout": "0"}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        # Call request method which call requests.Session.send with timeout
        client.request('GET', url)

        # Verify requests.Session.send is called with expected timeout
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument
        
    def test_request_timeout_for_zero_int_value_in_config(self, mocked_request, mock_send, mock_sleep):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """       
        config = {'token': '123', "request_timeout": 0}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        # Call request method which call requests.Session.send with timeout
        client.request('GET', url)

        # Verify requests.Session.send is called with expected timeout
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

    def test_no_request_timeout_value_in_config(self, mocked_request, mock_send, mock_sleep):
        """
        Verify that if request_timeout is not provided in config then default value is used
        """       
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        # Call request method which call requests.Session.send with timeout
        client.request('GET', url)

        # Verify requests.Session.send is called with expected timeout
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs.get('timeout'), REQUEST_TIMEOUT_INT) # Verify timeout argument

@patch('time.sleep')
@patch('requests.Session.send', side_effect = requests.exceptions.Timeout)
@patch('requests.Request.prepare')
class TestRequestTimeoutsBackoff(unittest.TestCase):
    
    endpoint = "forms"

    def test_connection_error_backoff(self, mocked_request, mock_send, mock_sleep):
        """
            Verify request function is backoff for 5 times on Timeout exceeption
        """
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except requests.exceptions.Timeout as e:
            # Verify that request.Session.send called 5 times
            self.assertEqual(mock_send.call_count, 5)
    
@patch('time.sleep')
@patch('requests.Session.send', side_effect = requests.exceptions.ConnectionError)
@patch('requests.Request.prepare')
class TestConnectionErrorBackoff(unittest.TestCase):
    
    endpoint = "forms"

    def test_connection_error_backoff(self, mocked_request, mock_send, mock_sleep):
        """
            Verify request function is backoff for 5 times on ConnectionError exceeption
        """
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except requests.exceptions.ConnectionError as e:
            # Verify that request.Session.send called 5 times
            self.assertEqual(mock_send.call_count, 5)
    