import unittest
from unittest import mock
from parameterized import parameterized

from tap_typeform.streams import get_bookmark, get_min_bookmark, write_bookmarks, write_records


class TestGetBookmark(unittest.TestCase):
    """
    Test `get_bookmark` function.
    """
    state = {
        "bookmarks": {
            "forms": {"last_updated_at": "2020-01-01T00:00:00Z"},
            "landings": {
                "form1": {"landed_at": "2021-01-01T00:00:00Z"},
                "form2": {"landed_at": "2021-02-01T00:00:00Z"},
            }
        }
    }

    @parameterized.expand([
        (state, "forms", None, "last_updated_at", "2020-01-01T00:00:00Z"),
        ({"bookmarks": {}}, "forms", None, "last_updated_at", "2018-01-01T00:00:00Z"),
        (state, "landings", "form2", "landed_at", "2021-02-01T00:00:00Z"),
        (state, "landings", "form3", "landed_at", "2018-01-01T00:00:00Z"),
    ])
    def test_get_bookmark(self, mock_state, stream, form_id, bookmark_key, expected_bookamrk):
        """
        Test various scenarios for a bookmark with and without form_ids.
        """
        final_bookmark = get_bookmark(mock_state, stream, form_id, bookmark_key, "2018-01-01T00:00:00Z")

        # Verify that returned bookmark is exected
        self.assertEqual(final_bookmark, expected_bookamrk)

class TestGetMinBookmark(unittest.TestCase):
    """
    Test `get_min_bookmark` function.
    """
    state = {
        "bookmarks": {
            "forms": {"last_updated_at": "2020-01-01T00:00:00Z"},
            "landings": {
                "form1": {"landed_at": "2021-01-01T00:00:00Z"},
                "form2": {"landed_at": "2021-02-01T00:00:00Z"},
            },
            "answers": {
                "form1": {"landed_at": "2021-03-01T00:00:00Z"},
                "form2": {"landed_at": "2020-11-01T00:00:00Z"},
            }
        }
    }

    @parameterized.expand([
        (state, 'forms', ['forms'], "2022-06-01T00:00:00Z", None, "last_updated_at", "2020-01-01T00:00:00Z"),
        ({"bookmarks": {}}, 'forms', ['forms'], "2022-06-01T00:00:00Z", None, "last_updated_at", "2018-01-01T00:00:00Z"),
        (state, 'landings', ['landings'], "2022-06-01T00:00:00Z", "form2", "landed_at", "2021-02-01T00:00:00Z"),
        (state, 'landings', ['landings', 'answers'], "2022-06-01T00:00:00Z", "form1", "landed_at", "2021-01-01T00:00:00Z"),
        (state, 'landings', ['landings', 'answers'], "2022-06-01T00:00:00Z", "form2", "landed_at", "2020-11-01T00:00:00Z"),
        (state, 'landings', ['answers'], "2022-06-01T00:00:00Z", "form1", "landed_at", "2021-03-01T00:00:00Z"),
    ])
    def test_min_bookmark(self, state, stream, selected_streams, bookmark,
                form_id, bookmark_key, exected_bookmark):
        """
        Test that returned bookmark is a minimum of selected parent-child streams.
        """
        return_bookmark = get_min_bookmark(stream, selected_streams, bookmark,
                            "2018-01-01T00:00:00Z", state, form_id, bookmark_key)

        # Verify that returned bookmark is exected
        self.assertEqual(return_bookmark, exected_bookmark)

class TestWriteBookmark(unittest.TestCase):
    """
    Test `write_bookmark` function
    """

    state1 = {
        "bookmarks": {"forms": {"last_updated_at": "2020-01-01T00:00:00Z"}}
    }
    expected_state1 = {
        "bookmarks": {"forms": {"last_updated_at": "2020-02-01T00:00:00Z"}}
    }

    state2 = {
        "bookmarks": {"landings": {"form1": {"landed_at": "2020-01-01T00:00:00Z"}}}
    }
    expected_state2 = {
        "bookmarks": {"landings": {"form1": {"landed_at": "2020-03-01T00:00:00Z"}}}
    }
    state3 = {
        "bookmarks": {"landings": {"form1": {"landed_at": "2020-01-01T00:00:00Z"}}}
    }
    expected_state3 = {
        "bookmarks": {
            "landings": {"form1": {"landed_at": "2020-01-01T00:00:00Z"}},
            "answers": {"form4": {"landed_at": "2020-07-01T00:00:00Z"}}
        }
    }
    expected_state4 = {
        "bookmarks": {"answers": {"form3": {"landed_at": "2020-05-01T00:00:00Z"}}}
    }

    @parameterized.expand([
        (state1, 'forms', [], None, '2021-10-01T00:00:00Z', state1),
        (state1, 'forms', ['forms'], None, '2020-02-01T00:00:00Z', expected_state1),
        (state2, 'answers', [], "form2", '2021-03-01T00:00:00Z', state2),
        (state2, 'landings', ['landings'], "form1", '2020-03-01T00:00:00Z', expected_state2),
        (state3, 'landings', ['answers'], "form4", '2020-07-01T00:00:00Z', expected_state3),
        ({}, 'answers', ['answers'], "form3", '2020-05-01T00:00:00Z', expected_state4),
    ])
    def test_write_bookmark(self, state, stream, selected_streams, form_id, bookmark_value, expected_state):
        """
        Test function in various scenarios,
            - with and without form_ids
            - selected and not selected stream
            - selected child stream only
        """
        write_bookmarks(stream, selected_streams, form_id, bookmark_value, state)

        # Verify that the final state is equal to the expected state
        self.assertEqual(state, expected_state)

@mock.patch("tap_typeform.streams.singer.utils")
@mock.patch("tap_typeform.streams.singer.metadata")
@mock.patch("tap_typeform.streams.singer.write_record")
class TestWriteRecords(unittest.TestCase):
    """
    Test `write_records` function
    """
    
    catalog = {
        "schema": {},
        "metadata":[]
    }
    records_1 = [
        {"id": "abcd", "last_updated_at": "2022-07-05T06:53:30.687143Z"},
        {"id": "abcd1", "last_updated_at": "2022-07-05T07:53:30.687143Z"},
        {"id": "abcd2", "last_updated_at": "2022-07-05T08:53:30.687143Z"},
        {"id": "abcd3", "last_updated_at": "2022-07-05T09:53:30.687143Z"},
    ]
    @parameterized.expand([
        (records_1, 4),
        ([], 0),
    ])
    def test_write_records(self, mock_write_records, mock_metadata, mock_utils, records, count):
        """
        Test that function calls singer.write_records for each record
        """

        write_records(self.catalog, 'forms', records)

        # Verify singer.write_record called equal times as records count
        self.assertEqual(mock_write_records.call_count, count)
