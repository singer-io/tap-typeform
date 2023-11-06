import unittest
import os
import json
import requests
from unittest import mock

from tap_typeform.client import Client


http_response = {"refresh_token": "new_refresh_token",
                 "access_token": "new_access_token"}

test_config_path = "/tmp/test_config.json"


class Mockresponse:
    """ Mock response object class."""

    def __init__(self, status_code, raise_error, text=""):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = text

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        """ Response JSON method."""
        return self.text


def get_mock_http_response(status_code):
    """Return http mock response."""
    response = requests.Response()
    response.status_code = status_code
    return response


def get_response(status_code, raise_error=True, text=""):
    """ Returns required mock response. """
    return Mockresponse(status_code, raise_error=raise_error, text=text)


def write_new_config_file(**kwargs):
    test_config = {}
    with open(test_config_path, 'w') as config:
        for key, value in kwargs.items():
            test_config[key] = value
        config.write(json.dumps(test_config))


class TestDevMode(unittest.TestCase):
    def tearDown(self):
        if os.path.isfile(test_config_path):
            os.remove(test_config_path)

    @mock.patch("requests.Session.request")
    def test_dev_mode_not_enabled(self, mock_post_request):
        test_config = {"refresh_token": "old_refresh_token",
                       "token": "old_access_token"}
        write_new_config_file(**test_config)
        mock_post_request.side_effect = [get_response(200, raise_error=False, text=http_response)]
        client = Client(config=test_config, config_path=test_config_path, dev_mode=False)
        self.assertEqual(client.refresh_token, "new_refresh_token")
        self.assertEqual(client.access_token, "new_access_token")

    @mock.patch("requests.Session.request")
    def test_dev_mode_enabled(self, mock_post_request):
        test_config = {"refresh_token": "old_refresh_token",
                       "token": "old_access_token"}
        write_new_config_file(**test_config)
        mock_post_request.side_effect = [get_response(200, raise_error=False, text=http_response)]
        client = Client(config=test_config, config_path=test_config_path, dev_mode=True)
        self.assertEqual(client.refresh_token, "old_refresh_token")
        self.assertEqual(client.access_token, "old_access_token")


    @mock.patch("requests.Session.request")
    def test_no_refresh_token_not_dev_mode_enabled(self, mock_post_request):
        test_config = {"token": "old_access_token"}
        write_new_config_file(**test_config)
        mock_post_request.side_effect = [get_response(200, raise_error=False, text=http_response)]
        client = Client(config=test_config, config_path=test_config_path, dev_mode=False)
        self.assertIsNone(client.refresh_token)
        self.assertEqual(client.access_token, "old_access_token")

    @mock.patch("requests.Session.request")
    def test_no_refresh_token_dev_mode_enabled(self, mock_post_request):
        test_config = {"token": "old_access_token"}
        write_new_config_file(**test_config)
        mock_post_request.side_effect = [get_response(200, raise_error=False, text=http_response)]
        client = Client(config=test_config, config_path=test_config_path, dev_mode=True)
        self.assertIsNone(client.refresh_token)
        self.assertEqual(client.access_token, "old_access_token")
