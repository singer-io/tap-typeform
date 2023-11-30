import os
import datetime
import dateutil.parser
import pytz

from tap_tester import runner, menagerie, connections

from base import TypeformBaseTest


class TypeformBookmarks(TypeformBaseTest):

    @staticmethod
    def name():
        return "tap_tester_typeform_using_shared_token_chaining"

    @staticmethod
    def convert_state_to_utc(date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted UTC DateTime,
        to compare against JSON formatted DateTime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%S.%fZ")


    def test_run(self):
        expected_streams = self.expected_streams()

        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        ### First Sync
        ##########################################################################
        self.start_date_1 = "2022-07-20T00:00:00Z"
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=3)

        self.start_date = self.start_date_1
        conn_id = connections.ensure_connection(self, original_properties=False, payload_hook=self.preserve_refresh_token)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries, select_all_fields=True)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update State Between Syncs
        ##########################################################################

        new_states = {'bookmarks': dict()}
        simulated_states = {
            'forms': {'last_updated_at': '2022-07-22T16:00:47Z'},
            'submitted_landings': {'mVn2wE55': {'submitted_at': '2022-07-21T00:00:00Z'},
                                   'xJ8emTTy': {'submitted_at': '2022-07-24T16:49:54Z'},
                                   'cfyTOGxc': {'submitted_at': '2022-07-24T16:49:54Z'}},
            'unsubmitted_landings': {'mVn2wE55': {'landed_at': '2022-07-20T10:32:15.000000Z'},
                                     'xJ8emTTy': {'landed_at': '2022-07-24T15:49:54Z'},
                                     'cfyTOGxc': {'landed_at': '2022-07-24T16:49:54Z'}},
            'answers': {'mVn2wE55': {'submitted_at': '2022-07-21T00:00:00Z'},
                        'xJ8emTTy': {'submitted_at': '2022-07-24T16:49:54Z'},
                        'cfyTOGxc': {'submitted_at': '2022-07-24T16:49:54Z'}}
        }
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        conn_id_2 = connections.ensure_connection(self, original_properties=False, payload_hook=self.preserve_refresh_token)
        menagerie.set_state(conn_id_2, new_states)

        for stream in simulated_states.keys():
            for state_key, state_value in simulated_states[stream].items():
                if stream not in new_states['bookmarks']:
                    new_states['bookmarks'][stream] = {}
                if state_key not in new_states['bookmarks'][stream]:
                    new_states['bookmarks'][stream][state_key] = state_value

        ##########################################################################
        ### Second Sync
        ##########################################################################
        self.start_date = self.start_date_2

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id_2)

        # Table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs
                                        if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        second_sync_record_count = self.run_and_verify_sync(conn_id_2)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id_2)

        ##########################################################################
        ### Test By Stream
        ##########################################################################

        for stream in expected_streams:
            with self.subTest(stream=stream):
                expected_replication_method = expected_replication_methods[stream]
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                # Expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_messages = [record.get('data') for record in
                                    first_sync_records.get(stream).get('messages')
                                    if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']

                if expected_replication_method == self.INCREMENTAL:

                    replication_key = next(iter(expected_replication_keys[stream]))

                    if stream != 'forms':
                        for form_key in self.get_forms():
                            first_bookmark_value = first_bookmark_key_value.get(form_key, {}).get(replication_key)
                            second_bookmark_value = second_bookmark_key_value.get(form_key, {}).get(replication_key)

                            first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                            second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)

                            simulated_bookmark_value = new_states['bookmarks'][stream][form_key][replication_key]
                            simulated_bookmark_minus_lookback = simulated_bookmark_value

                            # Verify the second sync bookmark is Greater or Equal to the first sync bookmark
                            self.assertGreaterEqual(second_bookmark_value, first_bookmark_value) # new responses could be picked up for the form in the second sync

                            # The `answers` stream respects the parent stream's bookmark, hence skipped this stream.
                            if stream != 'answers':
                                for record in second_sync_messages:

                                    # Verify the second sync records respect the previous (simulated) bookmark value
                                    if record.get('_sdc_form_id') == form_key:
                                        replication_key_value = record.get(replication_key)
                                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_minus_lookback,
                                                            msg="Second sync records do not respect the previous bookmark.")
                                    else:
                                        continue

                                    # Verify the second sync bookmark value is the max replication key value for a given stream
                                    self.assertLessEqual(
                                        replication_key_value, second_bookmark_value,
                                        msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                                    )

                                for record in first_sync_messages:
                                    # Verify the first sync bookmark value is the max replication key value for a given stream
                                    if record.get('_sdc_form_id') == form_key:
                                        replication_key_value = record.get(replication_key)
                                        self.assertLessEqual(
                                            replication_key_value, first_bookmark_value,
                                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                                        )

                            # Verify the number of records in the 2nd sync is less than the first
                            self.assertLess(second_sync_count, first_sync_count)

                    else:
                        # collect information specific to incremental streams from syncs 1 & 2
                        first_bookmark_value = first_bookmark_key_value.get(replication_key)
                        second_bookmark_value = second_bookmark_key_value.get(replication_key)
                        first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                        second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)
                        simulated_bookmark_value = new_states['bookmarks'][stream][replication_key]
                        simulated_bookmark_minus_lookback = simulated_bookmark_value

                        for record in second_sync_messages:

                            # Verify the second sync records respect the previous (simulated) bookmark value
                            replication_key_value = record.get(replication_key)
                            self.assertGreaterEqual(replication_key_value, simulated_bookmark_minus_lookback,
                                                    msg="Second sync records do not respect the previous bookmark.")

                            # Verify the second sync bookmark value is the max replication key value for a given stream
                            self.assertLessEqual(
                                replication_key_value, second_bookmark_value_utc,
                                msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                            )

                        for record in first_sync_messages:

                            # Verify the first sync bookmark value is the max replication key value for a given stream
                            replication_key_value = record.get(replication_key)
                            self.assertLessEqual(
                                replication_key_value, first_bookmark_value_utc,
                                msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                            )

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Greater or Equal to the first sync bookmark
                    self.assertGreaterEqual(second_bookmark_value, first_bookmark_value) # new responses could be picked up for the form in the second sync

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method)
                    )

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
