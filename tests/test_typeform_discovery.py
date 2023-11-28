"""Test tap discovery mode and metadata."""
import re

from tap_tester import menagerie, connections

from base import TypeformBaseTest


class DiscoveryTest(TypeformBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_typeform_using_shared_token_chaining"

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.
        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • Verify there is only 1 top level breadcrumb
        • Verify there are no duplicate metadata entries
        • Verify replication key(s)
        • Verify primary key(s)
        • Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • Verify the actual replication matches our expected replication method
        • Verify that primary, replication and foreign keys
          are given the inclusion of automatic.
        • Verify that all other fields have inclusion of available metadata.
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        # Verify that there are catalogs found
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify ensure the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys
                expected_replication_method = self.expected_replication_method()[stream]

                # collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                if len(stream_properties) == 0:
                    stream_properties.append({})
                actual_primary_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])
                )
                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                )

                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                ##########################################################################
                ### metadata assertions
                ##########################################################################

                # Verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # Verify there are no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)), msg = "duplicates in the fields retrieved")

                # Verify replication key(s) match expectations
                self.assertEqual(expected_replication_keys, actual_replication_keys,
                                 msg="expected replication key {} but actual is {}".format(
                                     expected_replication_keys, actual_replication_keys))

                # Verify primary key(s) match expectations
                self.assertSetEqual(
                    expected_primary_keys, actual_primary_keys,
                )

                # Verify the replication method matches our expectations
                self.assertEqual(expected_replication_method, actual_replication_method,
                                    msg="The actual replication method {} doesn't match the expected {}".format(
                                        actual_replication_method, expected_replication_method))

                # Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if expected_replication_keys:
                    self.assertEqual(self.INCREMENTAL, actual_replication_method)
                else:
                    self.assertEqual(self.FULL_TABLE, actual_replication_method)


                # Verify that primary keys and replication keys are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # Verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata")
