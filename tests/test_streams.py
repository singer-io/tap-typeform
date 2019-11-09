import datetime
import pytz
import unittest
from unittest.mock import patch

from tap_typeform.context import Context
from tap_typeform.streams import sync_forms


class TestStreams(unittest.TestCase):
    def setUp(self):
        self.form_id = "test_form_id"
        self.config = {
            "forms": self.form_id,
            "incremental_range": "hourly",
            "start_date": "2019-11-02 10:00:00",
            "end_date": "2019-11-02 12:00:00",
            "token": "token",
        }
        self.state = {"bookmark": {"form_id": self.form_id}}

        self.atx = Context(self.config, self.state)
        self.atx.selected_stream_ids = ["answers"]

    @patch("tap_typeform.streams.sync_form")
    def test_sync_form(self, mock_sync_form):
        mock_sync_form.return_value = [10, ""]  # To prevent Value Error

        sync_forms(self.atx)

        self.assertEqual(mock_sync_form.call_count, 2)

    @patch("tap_typeform.streams.sync_form")
    def test_bookmarked_date_is_correct(self, mock_sync_form):
        mock_sync_form.return_value = [10, ""]  # To prevent Value Error
        end_date = pytz.utc.localize(
            datetime.datetime.strptime(self.config.get("end_date"), "%Y-%m-%d %H:%M:%S")
        )

        sync_forms(self.atx)

        date_to_resume = self.atx.state.get("bookmarks").get(self.form_id, {}).get("date_to_resume")
        expected_date_to_resume = end_date.strftime("%Y-%m-%d %H:%M:%S")

        assert date_to_resume == expected_date_to_resume

    @patch("tap_typeform.streams.sync_form")
    def test_when_end_date_is_not_round(self, mock_sync_form):
        self.config["end_date"] = "2019-11-02 11:30:00"
        self.atx = Context(self.config, self.state)
        self.atx.selected_stream_ids = ["answers"]
        mock_sync_form.return_value = [10, ""]  # To prevent Value Error

        sync_forms(self.atx)

        date_to_resume = self.atx.state.get("bookmarks").get(self.form_id, {}).get("date_to_resume")
        expected_date_to_resume = "2019-11-02 11:00:00"  # Or is it 11:30:00 ?

        assert date_to_resume == expected_date_to_resume


if __name__ == "__main__":
    unittest.main()
