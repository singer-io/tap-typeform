import json
import os
import unittest
from unittest import mock
from parameterized import parameterized

import requests
from tap_typeform.client import ERROR_CODE_EXCEPTION_MAPPING
import tap_typeform.client as client_


test_config = {
    "client_id": "client_id",
    "client_secret": "client_secret",
    "access_token": "old_access_token"
}
test_config_path = "/tmp/test_config.json"

def write_new_config_file():
    with open(test_config_path, 'w') as config:
        # Reset tokens while writing the test config
        test_config["access_token"] = "old_access_token"
        config.write(json.dumps(test_config))

class Mockresponse:
    def __init__(self, resp, status_code, content=[], headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("sample message")


def get_mock_http_response(status_code, contents):
    """Return http mock response."""
    response = requests.Response()
    response.status_code = status_code
    response._content = contents.encode()
    return response

def mocked_badrequest_400_error(*args, **kwargs):
    json_decode_str = {"code": "VALIDATION_ERROR"}

    return Mockresponse(json_decode_str, 400, raise_error=True)


def mocked_unauthorized_401_error(*args, **kwargs):
    json_decode_str = {"code": "UNAUTHORIZED", "description": "Authentication credentials not found on the Request Headers"}

    return Mockresponse(json_decode_str, 401, raise_error=True)


def mocked_forbidden_403_exception(*args, **kwargs):
    json_decode_str = {"code": "AUTHENTICATION_FAILED", "description": "Authentication failed"}

    return Mockresponse(json_decode_str, 403, raise_error=True)


def mocked_notfound_404_error(*args, **kwargs):
    json_decode_str = {"code": "NOT_FOUND", "description": "Endpoint not found"}

    return Mockresponse(json_decode_str, 404, raise_error=True)


def mocked_failed_429_request(*args, **kwargs):
    json_decode_str = ''
    headers = {"Retry-After": 1000, "X-Rate-Limit-Problem": "day"}
    return Mockresponse(json_decode_str, 429, headers=headers, raise_error=True)


def mocked_internalservererror_500_error(*args, **kwargs):
    json_decode_str = {}

    return Mockresponse(json_decode_str, 500, raise_error=True)


def mocked_not_available_503_error(*args, **kwargs):
    json_decode_str = {}

    return Mockresponse(json_decode_str, 503, raise_error=True)

def mocked_not_available_504_error(*args, **kwargs):
    json_decode_str = {}

    return Mockresponse(json_decode_str, 504, raise_error=True)

@mock.patch("time.sleep")
@mock.patch('tap_typeform.client.requests.Session.get')
class TestClientErrorHandling(unittest.TestCase):
    """
    Test handling of 4xx and 5xx errors with a proper error message.
    """

    endpoint = "forms"

    def tearDown(self):
        if os.path.isfile(test_config_path):
            os.remove(test_config_path)

    @parameterized.expand([
        (client_.TypeformBadRequestError, mocked_badrequest_400_error, 400),
        (client_.TypeformUnauthorizedError, mocked_unauthorized_401_error, 401),
        (client_.TypeformForbiddenError, mocked_forbidden_403_exception, 403),
        (client_.TypeformNotFoundError, mocked_notfound_404_error, 404),
        (client_.TypeformTooManyError, mocked_failed_429_request, 429),
        (client_.TypeformInternalError, mocked_internalservererror_500_error, 500),
        (client_.TypeformNotAvailableError, mocked_not_available_503_error, 503),
        (client_.TypeformError, mocked_not_available_504_error, 504),
    ])
    def test_error_handling(self, mock_session, mock_sleep, error, mock_response, err_code):
        """
        Test error is raised with an expected error message.
        """
        write_new_config_file()
        client = client_.Client(test_config, test_config_path, False)
        url = client.build_url(self.endpoint)
        mock_session.side_effect=mock_response
        error_message = ERROR_CODE_EXCEPTION_MAPPING.get(err_code, {}).get("message", "")

        expected_error_message = "HTTP-error-code: {}, Error: {}".format(err_code, error_message)
        with self.assertRaises(error) as e:
            client.request(url)

        # Verifying the message formed for the custom exception
        self.assertEqual(str(e.exception), expected_error_message)

    @mock.patch("tap_typeform.client.raise_for_error")
    @mock.patch("tap_typeform.client.LOGGER.info")
    def test_success_response(self, mock_logger, mock_raise_error, mock_session, mock_sleep):
        """
        Test that for success response, error is not raised
        """
        write_new_config_file()
        client = client_.Client(test_config, test_config_path, False)
        mock_session.return_value=get_mock_http_response(200, '{"total_items": 10}')
        client.request("")

        # Verify `raised_for_error` is not called
        self.assertFalse(mock_raise_error.called)

        # Verify `raw data item` logger is called
        mock_logger.assert_called_with("raw data items= 10")

@mock.patch("time.sleep")
@mock.patch('tap_typeform.client.requests.Session.get')
class TestClientBackoffHandling(unittest.TestCase):
    """
    Test handling of backoff for Timeout, ConnectionError, ChunkEncoding, 5xx, 429 errors.
    """

    endpoint = "forms"

    def tearDown(self):
        if os.path.isfile(test_config_path):
            os.remove(test_config_path)

    @parameterized.expand([
        (requests.exceptions.ConnectionError, requests.exceptions.ConnectionError, 5),
        (requests.exceptions.Timeout, requests.exceptions.Timeout, 5),
        (requests.exceptions.ChunkedEncodingError, requests.exceptions.ChunkedEncodingError, 3),
        (client_.TypeformInternalError, mocked_internalservererror_500_error, 3),
        (client_.TypeformNotAvailableError, mocked_not_available_503_error, 3),
        (client_.TypeformTooManyError, mocked_failed_429_request, 3),
    ])
    def test_back_off_error_handling(self, mock_session, mock_sleep, error,mock_response, expected_call_count):
        """
        Test handling of backoff that function is retrying expected times
        """
        mock_session.side_effect = mock_response
        write_new_config_file()
        client = client_.Client(test_config, test_config_path, False)
        url = client.build_url(self.endpoint)
        with self.assertRaises(error):
            client.request(url)

        # Verify `client.requests` backoff expected times
        self.assertEqual(mock_session.call_count, expected_call_count)
