import unittest
from unittest import mock
from tap_typeform.discover import discover
from singer.catalog import Catalog


class TestDiscover(unittest.TestCase):
    """Test `discover` function."""

    def _make_client(self):
        client = mock.MagicMock()
        client.request.return_value = {'items': [], 'page_count': 0}
        return client

    @mock.patch("tap_typeform.discover._apply_access_checks")
    def test_discover(self, mock_access_checks):
        mock_access_checks.return_value = None
        return_catalog = discover(self._make_client())

        self.assertIsInstance(return_catalog, Catalog)

    @mock.patch("tap_typeform.discover._apply_access_checks")
    @mock.patch("tap_typeform.discover.Schema")
    @mock.patch("tap_typeform.discover.LOGGER.error")
    def test_discover_error_handling(self, mock_logger, mock_schema, mock_access_checks):
        """Test discover function if exception arises."""
        mock_access_checks.return_value = None
        mock_schema.from_dict.side_effect = [Exception]
        with self.assertRaises(Exception):
            discover(self._make_client())

        # Verify logger called 3 times when an exception arises.
        self.assertEqual(mock_logger.call_count, 3)
