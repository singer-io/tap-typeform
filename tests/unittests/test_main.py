import unittest
from unittest import mock
from tap_typeform import (main, NoFormsProvidedError,
                            FormMistmatchError, validate_form_ids)
from singer.catalog import Catalog


class MockArgs:
    """Mock args object class"""

    def __init__(self, config = None, catalog = None, state = {}, discover = False) -> None:
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
        mock_args.return_value = MockArgs(discover = True, config = self.mock_config)
        main()

        self.assertTrue(mock_discover.called)
        self.assertFalse(mock_sync.called)


    def test_sync_with_catalog(self, mock_sync, mock_discover, mock_args, mock_validate):
        """
        Test sync mode with catalog given in args.
        """

        mock_args.return_value = MockArgs(config=self.mock_config, catalog=Catalog.from_dict(self.mock_catalog))
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(mock.ANY, self.mock_config, {}, self.mock_catalog)

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
        mock_sync.assert_called_with(mock.ANY, self.mock_config, {}, {"schema": "", "metadata": ""})

        # verify `_discover` function is  called
        self.assertTrue(mock_discover.called)

    def test_sync_with_state(self, mock_sync, mock_discover, mock_args, mock_validate):
        """
        Test sync mode with the state given in args.
        """
        mock_state = {"bookmarks": {"projects": ""}}
        mock_args.return_value = MockArgs(config=self.mock_config, catalog=Catalog.from_dict(self.mock_catalog), state=mock_state)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(mock.ANY, self.mock_config, mock_state, self.mock_catalog)


@mock.patch("tap_typeform.Forms")
class TestValidateFormIds(unittest.TestCase):
    """
    Test `validate_form_ids` function.
    """
    
    def test_all_correct_forms(self, mock_forms):
        """
        Test when proper form ids are passed, No error raised.
        """
        input_value = "form1,form2"
        config = {"forms": input_value}
        mock_forms.return_value.get_forms.return_value = [[{'id': 'form1'}, {'id': 'form2'}, {'id': 'form3'}]]

        forms = validate_form_ids(None, config)
        self.assertEqual(input_value, ','.join(forms))

    # UPDATE: fetch all forms
    # also: test for no value in forms field?
    def test_no_form_given(self, mock_forms):
        """
        Test when no forms are given in config, an error is raised with an expected message.
        """
        config = {}
        forms_from_typeform = [{'id': 'form1'}, {'id': 'form2'}, {'id': 'form3'}]
        mock_forms.return_value.get_forms.return_value = [forms_from_typeform]
        forms = validate_form_ids(None, config)
        # with self.assertRaises(NoFormsProvidedError) as e:
            # validate_form_ids(None, config)

        # Verify exception raised with expected error message
        # self.assertEqual(str(e.exception), "No forms were provided in the config")
        self.assertListEqual(forms, [f['id'] for f in forms_from_typeform])
    
    def test_mismatch_forms(self, mock_forms):
        """
        Test wrong form ids given in config raise MismatchError.
        """
        config = {"forms": "form1,form4"}
        mock_forms.return_value.get_forms.return_value = [[{'id': 'form1'}, {'id': 'form2'}, {'id': 'form3'}]]
        
        with self.assertRaises(FormMistmatchError) as e:
            validate_form_ids(None, config)

        # Verify exception raised with expected error message
        self.assertEqual(str(e.exception), "FormMistmatchError: forms {} not returned by API".format({"form4"}))
