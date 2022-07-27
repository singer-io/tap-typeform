import unittest
from unittest import mock
from parameterized import parameterized
from tap_typeform.client import Client
from tap_typeform.streams import Forms, Landings, Questions, Answers

def get_stream_catalog(stream_name, selected = False):
    """
    Returns catalog for each stream.
    """
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

catalogs = [
    get_stream_catalog("questions", True),
    get_stream_catalog("landings", True),
    get_stream_catalog("answers", True),
    get_stream_catalog("forms", True),
]

class TestFullTableStream(unittest.TestCase):
    """
    Test Full table streams.
    """

    @mock.patch("tap_typeform.streams.write_records")
    @mock.patch("tap_typeform.client.Client.request")
    def test_sync_object(self, mock_request, mock_write_records):
        """
        Test `sync_obj` for full table streams.
        """
        client = Client({"token": ""})
        test_stream = Questions()
        expected_records = [
            {"id": 1, "question_id": 1, "form_id": "form1"},
            {"id": 2, "question_id": 2, "form_id": "form1"},
        ]
        
        records = [
            {"id": 1},
            {"id": 2},
        ]
        mock_request.return_value = {
            "fields": records
        }

        test_stream.sync_obj(client, {}, catalogs, "form1", "", ['questions'])

        # Verify that write_records is called with the expected list of records
        mock_write_records.assert_called_with(
            get_stream_catalog("questions", True),
            "questions",
            expected_records
        )


@mock.patch("tap_typeform.client.Client.request")
@mock.patch("tap_typeform.streams.Landings.add_fields_at_1st_level")
class TestIncrementalStream(unittest.TestCase):
    """
    Test incremental streams methods.
    """
    
    @mock.patch("tap_typeform.streams.IncrementalStream.write_records")
    def test_sync_obj(self, mock_write_records, mock_add_field, mock_request):
        """
        Test `sync_obj` method of incremental streams. 
        """
        client = Client({"token": "", "fetch_uncompleted_forms": False})
        test_stream = Landings()

        records = [
            {"landing_id": 1, "landed_at": "", "answers": []},
            {"landing_id": 1, "landed_at": "", "answers": []},
        ]
        mock_request.side_effect = [
            {"items": records,"page_count": 2},
            {"items": records,"page_count": 1},
        ]

        test_stream.sync_obj(client, {}, catalogs, "form1", "", ['questions'])

        # Verify that write_records was called for both the page
        self.assertEqual(mock_write_records.call_count,2)

    @mock.patch("tap_typeform.streams.IncrementalStream.write_records")
    @mock.patch("tap_typeform.streams.pendulum")
    def test_uncompleted_forms_sync(self, mock_timestamp, mock_write_records, mock_add_field, mock_request):
        """
        Test the uncompleted forms sync for the `Landings` stream.
        """
        mock_timestamp.parse.return_value.timestamp.return_value = 0
        mock_write_records.return_value = ""
        client = Client({"token": "", "fetch_uncompleted_forms": True})
        test_stream = Landings()

        records = [
            {"landing_id": 1, "landed_at": "", "answers": []},
            {"landing_id": 1, "landed_at": "", "answers": []},
        ]
        mock_request.side_effect = [
            {"items": records,"page_count": 2},
            {"items": records,"page_count": 1},
            {"items": records,"page_count": 2},
            {"items": records,"page_count": 1},
        ]

        test_stream.sync_obj(client, {}, catalogs, "form1", "", ['questions'])

        # Verify that requests and `write_records` is called 4 times
        # (2 for completed forms, 2 for uncompleted forms)
        self.assertEqual(mock_request.call_count,4)
        self.assertEqual(mock_write_records.call_count,4)

    @mock.patch("tap_typeform.streams.singer.write_record")
    @mock.patch("tap_typeform.streams.Stream.sync_child_stream")
    def test_write_records(self, mock_sync_child, mock_write_record, mock_add_field, mock_request):
        """
        Test `write_records` method of incremental streams.
        """
        mock_sync_child.return_value = ""
        test_stream = Landings()

        records = [
            {"landing_id": 1, "landed_at": "", "answers": []},
            {"landing_id": 2, "landed_at": "", "answers": []},
            {"landing_id": 3, "landed_at": ""},
        ]

        test_stream.write_records(records, catalogs, ['answers', 'landings'],"form1", "", {}, "")

        # Verify write record was called for all records
        self.assertEqual(mock_write_record.call_count,3)

        # Verify that `sync_child` was called only for records containing `child_key`
        self.assertEqual(mock_sync_child.call_count,2)
        self.assertEqual(mock_sync_child.mock_calls[0], mock.call(records[0], mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock.ANY))
        self.assertEqual(mock_sync_child.mock_calls[1], mock.call(records[1], mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock.ANY, mock.ANY))

    @parameterized.expand([(['answers'], 1), ([], 0)])
    @mock.patch("tap_typeform.streams.write_records")
    @mock.patch("tap_typeform.streams.Answers.add_fields_at_1st_level")
    def test_child_sync_selected(self, selected_streams, call_count, mock_add_field, mock_write_records, mock_add_field2, mock_request):
        """
        Test child sync for the scenario:
            - If the child is selected, then `write_records` will be called
            - If the child is not selected, then `write_records` will not be called
        """
        test_stream = Landings()
        child_records = [
            {"field": {"id":1}},
            {"field": {"id":2}},
        ]

        record = {"landing_id": 1, "landed_at": "", "answers": child_records}

        test_stream.sync_child_stream(record, catalogs, {}, selected_streams, "form1", "", "")

        # Verify write records is called if the stream is selected
        self.assertEqual(mock_write_records.call_count, call_count)


@mock.patch("tap_typeform.client.Client.request")
class TestFormsStream(unittest.TestCase):
    """
    Test `sync_obj` method for Forms stream.
    """
    
    @mock.patch("tap_typeform.streams.IncrementalStream.write_records")
    def test_sync_obj(self, mock_write_records, mock_requests):
        mock_write_records.return_value = ""

        mock_requests.side_effect = [
            {"items": [], "page_count": 3},
            {"items": [], "page_count": 3},
            {"items": [], "page_count": 3},
        ]
        client = Client({"token": "", "fetch_uncompleted_forms": True})
        test_stream = Forms()
        
        test_stream.sync_obj(client, {}, catalogs, "", ['forms'])

        # Verify that write records called 3 time
        self.assertEqual(mock_write_records.call_count, 3)


class TestAddFieldAt1StLevel(unittest.TestCase):
    """
    Test `add_fields_at_1st_level` method for all streams.
    """

    answer_record = {
        "type": "text",
        "field": {
            "id": "asdf",
            "type": "short_text",
            "ref": ""
        },
        "text": ""
    }
    answer_expected_record = {
        "field": {
            "id": "asdf",
            "type": "short_text",
            "ref": ""
        },
        "landing_id": 1,
        "question_id": "asdf",
        "type": "short_text",
        "ref": "",
        "data_type": "text",
        "landed_at": "",
        "answer": "",
        "text": "",
        "_sdc_form_id": "form1"
    }

    landing_record = {
        "metadata": {
            "user_agent": "mozila",
            "platform": "other",
            "referer": "",
            "network_id": "",
            "browser": "default"
        }
    }
    landing_expected_record = {
        **landing_record,
        "user_agent": "mozila",
        "platform": "other",
        "referer": "",
        "network_id": "",
        "browser": "default",
        "hidden": "",
        "_sdc_form_id": "form1"
    }
    que_record= {
        "id": "que_id_1"
    }
    que_expected_record={
        "question_id": "que_id_1",
        "id": "que_id_1",
        "form_id": "form1",
    }
    @parameterized.expand([
        (Answers, answer_record, {"landing_id": 1,"landed_at": "", "_sdc_form_id": "form1"}, answer_expected_record),
        (Landings, landing_record, {"_sdc_form_id": "form1"}, landing_expected_record),
        (Questions, que_record, {"form_id": "form1"}, que_expected_record),
    ])
    def test_add_field(self, stream, record, aditional_data, expected_record):
        """
        Test method for various streams.
        """

        test_stream = stream()
        test_stream.add_fields_at_1st_level(record, aditional_data)

        # Verify that record updates as expected
        self.assertEqual(record, expected_record)
