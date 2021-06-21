import unittest
from unittest import mock

import requests
import tap_typeform.http as client_


class Mockresponse:
    def __init__(self, resp, status_code, content=[], headers=None, raise_error=False):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("sample message")


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


def mocked_notimplemented_501_error(*args, **kwargs):
    json_decode_str = {}

    return Mockresponse(json_decode_str, 501, raise_error=True)


def mocked_not_available_503_error(*args, **kwargs):
    json_decode_str = {}

    return Mockresponse(json_decode_str, 503, raise_error=True)


@mock.patch('requests.request', side_effect=Mockresponse)
class TestClientExceptionHandling(unittest.TestCase):
    """
    Test cases to verify if the exceptions are handled as expected while communicating with Xero Environment 
    """
    endpoint = "forms"

    @mock.patch('requests.request', side_effect=mocked_badrequest_400_error)
    def test_badrequest_400_error(self, mocked_session, mocked_badrequest_400_error):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except client_.TypeformBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.request', side_effect=mocked_unauthorized_401_error)
    def test_unauthorized_401_error(self, mocked_session, mocked_unauthorized_401_error):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except client_.TypeformUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: Invalid authorization credentials."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.request', side_effect=mocked_forbidden_403_exception)
    def test_forbidden_403_exception(self, mocked_session, mocked_forbidden_403_exception):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except client_.TypeformForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: User doesn't have permission to access the resource."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.request', side_effect=mocked_notfound_404_error)
    def test_notfound_404_error(self, mocked_session, mocked_notfound_404_error):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except client_.TypeformNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.request', side_effect=mocked_internalservererror_500_error)
    def test_internalservererror_500_error(self, mocked_session, mocked_internalservererror_500_error):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except client_.TypeformInternalError as e:
            expected_error_message = "HTTP-error-code: 500, Error: An unhandled error with the Xero API. Contact the Xero API team if problems persist."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.request', side_effect=mocked_not_available_503_error)
    def test_not_available_503_error(self, mocked_session, mocked_not_available_503_error):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except client_.TypeformNotAvailableError as e:
            expected_error_message = "HTTP-error-code: 503, Error: API service is currently unavailable."

            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.request', side_effect=mocked_failed_429_request)
    def test_too_many_requests_429(self, mocked_session, mocked_failed_429_request):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except client_.TypeformTooManyError as e:
            expected_error_message = "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded. Please retry after 1000 seconds"
            
            # Verifying the message formed for the custom exception
            self.assertEquals(str(e), expected_error_message)
            pass


    @mock.patch('requests.request', side_effect=mocked_failed_429_request)
    def test_too_many_requests_429_backoff_behavior(self, mocked_session, mocked_failed_429_request):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except (requests.HTTPError, client_.TypeformTooManyError) as e:
            pass

        #Verify daily limit should not backoff
        self.assertEqual(mocked_failed_429_request.call_count, 3)


    @mock.patch('requests.request', side_effect=mocked_internalservererror_500_error)
    def test_internalservererror_500_backoff_behaviour(self, mocked_session, mocked_internalservererror_500_error):
        config = {'token': '123'}
        client = client_.Client(config)
        url = client.build_url(self.endpoint)
        try:
            client.request('GET', url)
        except (requests.HTTPError, client_.TypeformInternalError) as e:
            pass

        self.assertEqual(mocked_internalservererror_500_error.call_count, 3)


if __name__ == '__main__':
    unittest.main()
