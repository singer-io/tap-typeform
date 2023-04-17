import unittest
from unittest import mock
from tap_typeform import (main,
                          FormMistmatchError, validate_form_ids)
from singer.catalog import Catalog


class MockArgs:
    """Mock args object class"""

    def __init__(self, config=None, catalog=None, state={}, discover=False) -> None:
        self.config = config
        self.catalog = catalog
        self.state = state
        self.discover = discover


@mock.patch("tap_typeform.validate_form_ids")
@mock.patch("singer.utils.parse_args")
@mock.patch("tap_typeform._discover")
@mock.patch("tap_typeform._sync")
class TestMainWorkflow(unittest.TestCase):
    """
    Test main function for discover mode.
    """

    mock_config = {"start_date": "", "token": ""}
    mock_catalog = {"streams": [{"stream": "landings", "schema": {}, "metadata": {}}]}

    def test_discover_with_config(self, mock_sync, mock_discover, mock_args, mock_validate):
        """
        Test `_discover` function is called for discover mode.
        """
        mock_discover.dump.return_value = dict()
        mock_args.return_value = MockArgs(discover=True, config=self.mock_config)
        main()

        self.assertTrue(mock_discover.called)
        self.assertFalse(mock_sync.called)

    def test_sync_with_catalog(self, mock_sync, mock_discover, mock_args, mock_validate):
        """
        Test sync mode with catalog given in args.
        """

        mock_args.return_value = MockArgs(config=self.mock_config,
                                          catalog=Catalog.from_dict(self.mock_catalog))
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(mock.ANY, self.mock_config, {}, self.mock_catalog, mock_validate.return_value)

        # verify `_discover` function is not called
        self.assertFalse(mock_discover.called)

    def test_sync_without_catalog(self, mock_sync, mock_discover, mock_args, mock_validate):
        """
        Test sync mode without catalog given in args.
        """

        catalog = mock_discover.return_value
        catalog.to_dict.return_value = {"schema": "", "metadata": ""}
        mock_args.return_value = MockArgs(config=self.mock_config)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(mock.ANY, self.mock_config, {}, {"schema": "", "metadata": ""}, mock_validate.return_value)

        # verify `_discover` function is  called
        self.assertTrue(mock_discover.called)

    def test_sync_with_state(self, mock_sync, mock_discover, mock_args, mock_validate):
        """
        Test sync mode with the state given in args.
        """
        mock_state = {"bookmarks": {"projects": ""}}
        mock_args.return_value = MockArgs(config=self.mock_config,
                                          catalog=Catalog.from_dict(self.mock_catalog),
                                          state=mock_state)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(mock.ANY, self.mock_config, mock_state, self.mock_catalog, mock_validate.return_value)


@mock.patch("tap_typeform.Forms")
class TestValidateFormIds(unittest.TestCase):
    """
    Test `validate_form_ids` function.
    """

    def test_all_correct_forms(self, mock_forms):
        """
        Test when proper form ids are passed, No error raised.
        """
        config = {"forms": "form1,form2"}
        mock_forms.return_value.get_forms.return_value = [
            [{'id': 'form1'}, {'id': 'form2'}, {'id': 'form3'}]]

        # Verify no exception was raised
        api_forms = validate_form_ids(None, config)

        # Assertion to test validate_form_ids return only the configured form IDs
        self.assertEqual(api_forms, {"form1", "form2"})

    def test_no_form_given(self, mock_forms):
        """
        Test when no forms are given in config, a statement is logged with an expected message.
        """
        config = {}
        mock_forms.return_value.get_forms.return_value = [
            [{'id': 'form1'}, {'id': 'form2'}, {'id': 'form3'}]]
        with self.assertLogs(level='INFO') as log_statement:
            api_forms = validate_form_ids(None, config)
            self.assertEqual(log_statement.output,
                             ['INFO:root:No form ids provided in config, fetching all forms'])

        # Assertion to make sure we call the get_forms function once
        self.assertEqual(mock_forms.return_value.get_forms.call_count, 1)

        # Assertion to test validate_form_ids returns all the form IDs from API response
        self.assertEqual(api_forms, {"form1", "form2", "form3"})


    def test_mismatch_forms(self, mock_forms):
        """
        Test wrong form ids given in config raise MismatchError.
        """
        config = {"forms": "form1,form4"}
        mock_forms.return_value.get_forms.return_value = [
            [{'id': 'form1'}, {'id': 'form2'}, {'id': 'form3'}]]

        with self.assertRaises(FormMistmatchError) as e:
            validate_form_ids(None, config)

        # Verify exception raised with expected error message
        self.assertEqual(str(e.exception),
                         "FormMistmatchError: forms {} not returned by API".format({"form4"}))
