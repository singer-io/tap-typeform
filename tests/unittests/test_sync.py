import unittest
from unittest import mock
from parameterized import parameterized
from tap_typeform.sync import (sync, get_stream_to_sync,
                                get_selected_streams, write_schemas, _forms_to_list)

def get_stream_catalog(stream_name, selected = False):
    return {
        "tap_stream_id": stream_name,
        "schema": {},
        "key_properties": "",
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "selected": selected
                }
            }
        ]
    }

records_count = {
    "forms": 0,
    "questions": 0,
    "submitted_landings": 0,
    "unsubmitted_landings": 0,
    "answers": 0
}

@mock.patch("tap_typeform.sync.pendulum")
@mock.patch("tap_typeform.sync.get_selected_streams")
@mock.patch("tap_typeform.sync.get_stream_to_sync")
@mock.patch("tap_typeform.sync.write_schemas")
class TestSyncFunction(unittest.TestCase):
    """
    Test sync function and verify proper sync object is called.
    """

    catalog = {'streams': {}}
    config = {'start_date': "START_DATE"}

    @mock.patch("tap_typeform.streams.Forms.sync_obj")
    def test_syncing_form(self, mock_sync_obj, mock_write_schema, mock_sync_streams, mock_selected_streams, mock_pendulum):
        """
        Test for `forms` stream, its sync_object is called with proper arguments.
        """
        mock_selected_streams.return_value = ['forms']
        mock_sync_streams.return_value = ['forms']

        sync(mock.Mock(), self.config, {}, self.catalog)

        # Verify that write schema is called once for one selected stream
        self.assertEqual(mock_write_schema.call_count, 1)
        mock_write_schema.assert_called_with('forms', self.catalog, ['forms'])

        # Verify that the expected sync object is called with proper args
        mock_sync_obj.assert_called_with(mock.ANY, {}, {}, "START_DATE", ['forms'], records_count)

    @mock.patch("tap_typeform.streams.SubmittedLandings.sync_obj")
    @mock.patch("tap_typeform.sync._forms_to_list")
    def test_only_child_selected(self, mock_form_list, mock_sync_obj,
                                    mock_write_schema, mock_sync_streams, mock_selected_streams, mock_pendulum):
        """
        Test for only child selected, parent sync object is called with proper arguments.
        """
        mock_form_list.return_value = ['form1']
        mock_selected_streams.return_value = ['answers']
        mock_sync_streams.return_value = ['submitted_landings', 'answers']

        sync(mock.Mock(), self.config, {}, self.catalog)

        # Verify that write schema is called once for one selected stream
        self.assertEqual(mock_write_schema.call_count, 1)
        mock_write_schema.assert_called_with('submitted_landings', self.catalog, ['answers'])

        # Verify that the expected sync object is called with proper args
        mock_sync_obj.assert_called_with(mock.ANY, {}, {}, 'form1', "START_DATE", ['answers'], records_count)

    @mock.patch("tap_typeform.streams.SubmittedLandings.sync_obj")
    @mock.patch("tap_typeform.sync._forms_to_list")
    def test_for_multiple_forms(self, mock_form_list, mock_sync_obj,
                                mock_write_schema, mock_sync_streams, mock_selected_streams, mock_pendulum):
        """
        Test for only child selected, parent sync object is called with proper arguments.
        """
        mock_form_list.return_value = ['form1', 'form2', 'form3']
        mock_selected_streams.return_value = ['submitted_landings']
        mock_sync_streams.return_value = ['submitted_landings']
        expected_calls = [
            mock.call(mock.ANY, {}, {}, 'form1', "START_DATE", ['submitted_landings'], records_count),
            mock.call(mock.ANY, {}, {}, 'form2', "START_DATE", ['submitted_landings'], records_count),
            mock.call(mock.ANY, {}, {}, 'form3', "START_DATE", ['submitted_landings'], records_count),
        ]

        sync(mock.Mock(), self.config, {}, self.catalog)

        # Verify that write schema is called once for one selected stream
        self.assertEqual(mock_write_schema.call_count, 1)
        mock_write_schema.assert_called_with('submitted_landings', self.catalog, ['submitted_landings'])
        
        # Verify that the expected sync object is called 3 times(for each form) with proper args
        self.assertEqual(mock_sync_obj.call_count, 3)
        for i in range(3):
            self.assertIn(mock_sync_obj.mock_calls[i], expected_calls)

class TestGetStreamsToSync(unittest.TestCase):
    """
    Test `get_streams_to_sync` function, that it returns expected.
    """

    def test_with_all_parent_streams(self):
        """
        Test for parents selected, the function returns the same list 
        """
        stream_list = ['forms', 'submitted_landings', 'questions']
        sync_streams = get_stream_to_sync(stream_list)

        # Verify that sync_stream list is as expected
        self.assertCountEqual(sync_streams, stream_list)

    def test_with_child_stream(self):
        """
        Test if the child is selected, and the parent is added in `sync_stream_list`.
        """
        expected_list = ['forms', 'submitted_landings', 'answers']
        sync_streams = get_stream_to_sync(['forms', 'answers'])

        # Verify that sync_stream list is as expected
        self.assertCountEqual(sync_streams, expected_list)

class TestGetselectedStreams(unittest.TestCase):
    """
    Test `get_selected_streams` function.
    """

    def test_selected_streams(self):
        """
        Test if any stream catalog is selected in the metadata, it should be in `selected_list`.
        """
        catalog = {
            "streams": [
                get_stream_catalog("forms", True),
                get_stream_catalog("submitted_landings"),
                get_stream_catalog("questions", True),
                get_stream_catalog("answers"),
            ]
        }
        expected_list = ["forms", "questions"]
        selected_list = get_selected_streams(catalog)

        # Verify that the selected stream list is as expected
        self.assertCountEqual(selected_list, expected_list)

@mock.patch("singer.write_schema")
class TestWriteSchemas(unittest.TestCase):
    """
    Test `write_schemas` that it writes schemas for selected stream.
    """

    catalog = {
        "streams": [
            get_stream_catalog("forms", True),
            get_stream_catalog("submitted_landings", True),
            get_stream_catalog("questions", True),
            get_stream_catalog("answers", True),
        ]
    }

    def test_only_parent_selected(self, mock_write_schema):
        """
        Test only parent's schema is written if the only parent is selected.
        """

        write_schemas("submitted_landings", self.catalog, ['forms', 'submitted_landings'])

        # Verify that write_schema is called for expected stream
        self.assertEqual(mock_write_schema.call_count, 1)
        mock_write_schema.assert_called_with('submitted_landings', mock.ANY, mock.ANY)

    def test_only_child_selected(self, mock_write_schema):
        """
        Test only parent's schema is written if the only parent is selected.
        """

        write_schemas("submitted_landings", self.catalog, ['answers'])

        # Verify if write_schema is called for expected stream
        self.assertEqual(mock_write_schema.call_count, 1)
        mock_write_schema.assert_called_with('answers', mock.ANY, mock.ANY)

    def test_for_child_selected(self, mock_write_schema):
        """
        Test parent and chid both schema is written if both are is selected.
        """
        expected_calls = [
            mock.call('answers', mock.ANY, ""),
            mock.call('submitted_landings', mock.ANY, ""),
        ]
        write_schemas("submitted_landings", self.catalog, ['answers', 'submitted_landings'])

        # Verify if write_schema is called for the expected stream
        self.assertEqual(mock_write_schema.call_count, 2)
        self.assertIn(mock_write_schema.mock_calls[0], expected_calls)
        self.assertIn(mock_write_schema.mock_calls[1], expected_calls)


class TestFormsListParsing(unittest.TestCase):
    """
    Test form list parsing and white space handling.
    """

    @parameterized.expand([
        ("form1,form2,form3",['form1', 'form2', 'form3']),
        ("form1, form2, form3",['form1', 'form2', 'form3']),
        ("    form1, form2   , form3   ",['form1', 'form2', 'form3']),
    ])
    def test_forms_to_list(self, forms, expected_list):
        """
        Test various test cases for form_list with whitespaces.
        """
        config = {'forms': forms}
        form_list = _forms_to_list(config)

        # Verify that returned list contains expected form_ids
        self.assertCountEqual(form_list, expected_list)
