import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie

from base import TypeformBaseTest, LOGGER

class TypeformStartDateTest(TypeformBaseTest):
    """
    Ensure both all expected streams respect the start date. Run tap in check mode, 
    run 1st sync with start date = 2019-03-01T00:00:00Z, run check mode and 2nd sync on a new connection with start date 2022-04-04T06:00:00Z.
    """
    
    @staticmethod
    def name():
        return "tap_tester_typeform_using_shared_token_chaining"

    start_date_1 = ""
    start_date_2 = ""
        
    def test_run(self):
        """
        Test that the start_date configuration is respected
        • Verify that a sync with a later start date has at least one record synced
        and less records than the 1st sync with a previous start date
        • Verify that each stream has less records than the earlier start date sync
        • Verify all data from later start data has bookmark values >= start_date
        """

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = "2022-07-21T00:00:00Z"
        self.start_date = self.start_date_1

        # Streams to verify start date tests
        expected_streams = self.expected_streams()

        expected_replication_methods = self.expected_replication_method()
        ##########################################################################
        # First Sync
        ##########################################################################

        # Instantiate connection
        conn_id_1 = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        # Run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # Table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # Run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        LOGGER.info("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False, payload_hook=self.preserve_refresh_token)

        # Run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # Table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # Run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)

                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream, {}).get('messages', [])
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream, {}).get('messages', [])
                                       if message.get('action') == 'upsert']

                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                if expected_replication_method == self.INCREMENTAL:

                    # Collect information specific to incremental streams from syncs 1 & 2
                    expected_replication_key = next(
                        iter(self.expected_replication_keys().get(stream, [])))
                    replication_dates_1 = [row.get('data').get(expected_replication_key) for row in
                                        synced_records_1.get(stream, {'messages': []}).get('messages', [])
                                        if row.get('data')]
                    replication_dates_2 = [row.get('data').get(expected_replication_key) for row in
                                        synced_records_2.get(stream, {'messages': []}).get('messages', [])
                                        if row.get('data')]

                    # Verify replication key is greater or equal to start_date for sync 1
                    for replication_date in replication_dates_1:

                        self.assertGreaterEqual(
                            replication_date, self.start_date_1,
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(self.start_date_1) +
                                "Record date: {} ".format(replication_date)
                        )

                    # Verify replication key is greater or equal to start_date for sync 2
                    for replication_date in replication_dates_2:

                        self.assertGreaterEqual(
                            replication_date, self.start_date_2,
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(self.start_date_2) +
                                "Record date: {} ".format(replication_date)
                        )

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertTrue(
                        primary_keys_sync_2.issubset(primary_keys_sync_1))

                if self.expected_metadata()[stream][self.OBEYS_START_DATE]:
                    
                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2
                    self.assertGreater(record_count_sync_1,
                                       record_count_sync_2)
                else:
                    
                    # Verify that the 2nd sync with a later start date replicates the same number of
                    # records as the 1st sync.

                    self.assertEqual(record_count_sync_2, record_count_sync_1)

                    # Verify by primary key values that the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
