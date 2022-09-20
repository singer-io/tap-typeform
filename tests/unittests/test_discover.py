import unittest
from unittest import mock
from tap_typeform.discover import discover
from singer.catalog import Catalog


class TestDiscover(unittest.TestCase):
    """Test `discover` function."""
    def test_discover(self):
        
        return_catalog = discover()
        
        self.assertIsInstance(return_catalog, Catalog)

    @mock.patch("tap_typeform.discover.Schema")
    @mock.patch("tap_typeform.discover.LOGGER.error")
    def test_discover_error_handling(self, mock_logger, mock_schema):
        """Test discover function if exception arises."""
        mock_schema.from_dict.side_effect = [Exception]
        with self.assertRaises(Exception):
            discover()

        # Verify logger called 3 times when an exception arises.
        self.assertEqual(mock_logger.call_count, 3)
