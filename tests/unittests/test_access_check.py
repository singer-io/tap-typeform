import unittest
from unittest.mock import MagicMock, patch

from tap_typeform.client import TypeformForbiddenError
from tap_typeform.discover import (
    _apply_access_checks,
    _prune_inaccessible_children,
    discover,
)
from tap_typeform.streams import (
    STREAMS,
    Answers,
    Forms,
    Questions,
    Stream,
    SubmittedLandings,
    UnsubmittedLandings,
)
from singer.catalog import Catalog


class TestCheckAccessBaseStream(unittest.TestCase):
    """Unit tests for Stream.check_access()."""

    def _make_client(self):
        client = MagicMock()
        client.build_url.side_effect = lambda ep: "https://api.typeform.com/{}".format(ep)
        client.request.return_value = {"items": [], "page_count": 0}
        return client

    def test_check_access_returns_true_for_child_stream(self):
        """Answers is a child of submitted_landings — always returns True."""
        stream = Answers(client=self._make_client())
        self.assertTrue(stream.check_access())

    def test_check_access_returns_true_when_no_probe_endpoint(self):
        """Base Stream with no check_access_endpoint always returns True."""
        stream = Stream(client=self._make_client())
        self.assertTrue(stream.check_access())

    def test_check_access_forms_returns_true_on_success(self):
        """Forms.check_access() returns True when API probe succeeds."""
        client = self._make_client()
        stream = Forms(client=client)
        result = stream.check_access()
        client.build_url.assert_called_once_with("forms")
        client.request.assert_called_once()
        self.assertTrue(result)

    def test_check_access_forms_returns_false_on_403(self):
        """Forms.check_access() returns False when API raises TypeformForbiddenError."""
        client = self._make_client()
        client.request.side_effect = TypeformForbiddenError("HTTP-error-code: 403, Error: Forbidden")
        stream = Forms(client=client)
        result = stream.check_access()
        self.assertFalse(result)

    def test_check_access_questions_returns_true_on_success(self):
        """Questions.check_access() returns True when forms endpoint succeeds."""
        client = self._make_client()
        stream = Questions(client=client)
        self.assertTrue(stream.check_access())

    def test_check_access_questions_returns_false_on_403(self):
        """Questions.check_access() returns False when forms endpoint returns 403."""
        client = self._make_client()
        client.request.side_effect = TypeformForbiddenError("HTTP-error-code: 403, Error: Forbidden")
        stream = Questions(client=client)
        self.assertFalse(stream.check_access())

    def test_check_access_submitted_landings_returns_true_on_success(self):
        """SubmittedLandings.check_access() returns True when forms endpoint succeeds."""
        client = self._make_client()
        stream = SubmittedLandings(client=client)
        self.assertTrue(stream.check_access())

    def test_check_access_submitted_landings_returns_false_on_403(self):
        """SubmittedLandings.check_access() returns False on 403."""
        client = self._make_client()
        client.request.side_effect = TypeformForbiddenError("HTTP-error-code: 403, Error: Forbidden")
        stream = SubmittedLandings(client=client)
        self.assertFalse(stream.check_access())

    def test_check_access_unsubmitted_landings_returns_true_on_success(self):
        """UnsubmittedLandings.check_access() returns True when forms endpoint succeeds."""
        client = self._make_client()
        stream = UnsubmittedLandings(client=client)
        self.assertTrue(stream.check_access())

    def test_check_access_unsubmitted_landings_returns_false_on_403(self):
        """UnsubmittedLandings.check_access() returns False on 403."""
        client = self._make_client()
        client.request.side_effect = TypeformForbiddenError("HTTP-error-code: 403, Error: Forbidden")
        stream = UnsubmittedLandings(client=client)
        self.assertFalse(stream.check_access())


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Unit tests for _prune_inaccessible_children()."""

    def _make_full_catalog(self):
        schemas = {name: {} for name in STREAMS}
        field_metadata = {name: [{"metadata": {}, "breadcrumb": []}] for name in STREAMS}
        return schemas, field_metadata

    def test_child_remains_when_parent_accessible(self):
        """answers stays in the catalog when submitted_landings is accessible."""
        schemas, field_metadata = self._make_full_catalog()
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertIn("answers", schemas)
        self.assertIn("answers", field_metadata)

    def test_answers_removed_when_submitted_landings_excluded(self):
        """answers is removed when submitted_landings is not in schemas."""
        schemas, field_metadata = self._make_full_catalog()
        schemas.pop("submitted_landings")
        field_metadata.pop("submitted_landings")
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertNotIn("answers", schemas)
        self.assertNotIn("answers", field_metadata)

    def test_parent_streams_unaffected_by_pruning(self):
        """Parent streams without a parent attribute are never removed by pruning."""
        schemas, field_metadata = self._make_full_catalog()
        schemas.pop("submitted_landings")
        field_metadata.pop("submitted_landings")
        _prune_inaccessible_children(schemas, field_metadata)
        for name in ("forms", "questions", "unsubmitted_landings"):
            self.assertIn(name, schemas)


class TestApplyAccessChecks(unittest.TestCase):
    """Unit tests for _apply_access_checks()."""

    def _make_client(self):
        client = MagicMock()
        client.build_url.side_effect = lambda ep: "https://api.typeform.com/{}".format(ep)
        client.request.return_value = {"items": [], "page_count": 0}
        return client

    def _make_full_catalog(self):
        schemas = {name: {} for name in STREAMS}
        field_metadata = {name: [{"metadata": {}, "breadcrumb": []}] for name in STREAMS}
        return schemas, field_metadata

    def test_all_streams_accessible_no_changes(self):
        """No streams are removed when all check_access() calls return True."""
        client = self._make_client()
        schemas, field_metadata = self._make_full_catalog()
        with patch("tap_typeform.streams.Stream.check_access", return_value=True):
            _apply_access_checks(client, schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), set(STREAMS.keys()))

    def test_inaccessible_stream_removed_from_catalog(self):
        """A stream whose check_access() returns False is removed."""
        client = self._make_client()
        schemas, field_metadata = self._make_full_catalog()

        def fake_check_access(self, form_id=None):
            return self.tap_stream_id != "forms"

        with patch("tap_typeform.streams.Stream.check_access", fake_check_access):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("forms", schemas)
        self.assertNotIn("forms", field_metadata)

    def test_answers_removed_when_submitted_landings_inaccessible(self):
        """answers child stream is removed when submitted_landings is inaccessible."""
        client = self._make_client()
        schemas, field_metadata = self._make_full_catalog()

        def fake_check_access(self, form_id=None):
            return self.tap_stream_id != "submitted_landings"

        with patch("tap_typeform.streams.Stream.check_access", fake_check_access):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("submitted_landings", schemas)
        self.assertNotIn("answers", schemas)
        self.assertNotIn("answers", field_metadata)

    def test_all_parent_streams_inaccessible_raises_forbidden_error(self):
        """TypeformForbiddenError is raised when every parent stream is inaccessible."""
        client = self._make_client()
        schemas, field_metadata = self._make_full_catalog()

        def fake_check_access(self, form_id=None):
            # child streams return True; all parents return False
            if self.parent:
                return True
            return False

        with patch("tap_typeform.streams.Stream.check_access", fake_check_access):
            with self.assertRaises(TypeformForbiddenError) as ctx:
                _apply_access_checks(client, schemas, field_metadata)

        self.assertIn("403", str(ctx.exception))

    def test_partial_inaccessible_does_not_raise(self):
        """No exception raised when at least one parent stream is accessible."""
        client = self._make_client()
        schemas, field_metadata = self._make_full_catalog()

        def fake_check_access(self, form_id=None):
            return self.tap_stream_id != "forms"

        with patch("tap_typeform.streams.Stream.check_access", fake_check_access):
            # Should not raise
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("forms", schemas)
        # Other parent streams still present
        self.assertIn("questions", schemas)

    def test_warning_logged_for_inaccessible_streams(self):
        """A warning is logged listing all excluded streams."""
        client = self._make_client()
        schemas, field_metadata = self._make_full_catalog()

        def fake_check_access(self, form_id=None):
            return self.tap_stream_id != "forms"

        with patch("tap_typeform.streams.Stream.check_access", fake_check_access), \
             patch("tap_typeform.discover.LOGGER.warning") as mock_warn:
            _apply_access_checks(client, schemas, field_metadata)

        # At least one warning call about excluded stream
        warning_messages = [str(call) for call in mock_warn.call_args_list]
        self.assertTrue(any("forms" in msg for msg in warning_messages))


class TestDiscoverWithAccessChecks(unittest.TestCase):
    """Integration-style tests for discover() with access check infrastructure."""

    def _make_client(self):
        client = MagicMock()
        client.build_url.side_effect = lambda ep: "https://api.typeform.com/{}".format(ep)
        client.request.return_value = {"items": [], "page_count": 0}
        return client

    @patch("tap_typeform.discover._apply_access_checks")
    def test_discover_returns_catalog_instance(self, mock_access_checks):
        """discover() returns a Catalog when all streams are accessible."""
        mock_access_checks.return_value = None
        client = self._make_client()
        result = discover(client)
        self.assertIsInstance(result, Catalog)

    @patch("tap_typeform.discover._apply_access_checks")
    def test_discover_passes_client_to_access_checks(self, mock_access_checks):
        """discover() passes the client object to _apply_access_checks()."""
        mock_access_checks.return_value = None
        client = self._make_client()
        discover(client)
        args = mock_access_checks.call_args[0]
        self.assertIs(args[0], client)

    @patch("tap_typeform.discover._apply_access_checks")
    def test_discover_catalog_contains_all_streams_when_fully_accessible(self, mock_access_checks):
        """discover() catalog entries match all STREAMS when nothing is excluded."""
        mock_access_checks.return_value = None
        client = self._make_client()
        result = discover(client)
        catalog_stream_ids = {entry.tap_stream_id for entry in result.streams}
        self.assertEqual(catalog_stream_ids, set(STREAMS.keys()))

    def test_discover_excludes_inaccessible_stream(self):
        """discover() excludes a stream that returns 403 during access check."""
        client = self._make_client()
        call_count = [0]

        def fake_request(url, params=None, **kwargs):
            call_count[0] += 1
            # First call (Forms probe) raises 403; subsequent calls succeed
            if call_count[0] == 1:
                raise TypeformForbiddenError("HTTP-error-code: 403, Error: Forbidden")
            return {"items": [], "page_count": 0}

        client.request.side_effect = fake_request

        result = discover(client)
        catalog_stream_ids = {entry.tap_stream_id for entry in result.streams}
        # forms was excluded
        self.assertNotIn("forms", catalog_stream_ids)
