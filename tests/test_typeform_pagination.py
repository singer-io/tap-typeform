import os
from math import ceil
import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TypeformBaseTest

class TypeformPaginationTest(TypeformBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    def name(self):
        return "tap_tester_typeform_using_shared_token_chaining"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'client_id': os.getenv('TAP_TYPEFORM_CLIENT_ID'),
            'start_date' : '2021-05-10T00:00:00Z',
            'forms': os.getenv('TAP_TYPEFORM_FORMS'),
            'incremental_range': 'daily',
            'page_size': self.PAGE_SIZE
        }
        return return_value

    def test_run(self):
        """
        • Verify that for each stream you can get multiple pages of data.  
        This requires we ensure more than 1 page of data exists at all times for any given stream.
        • Verify by pks that the data replicated matches the data we expect.
        """

        # Reduce page_size to 2 as unable to generate more data.
        expected_streams_1 = {'unsubmitted_landings'}
        self.run_test(expected_streams_1, page_size=2)

        # Reduce page_size to 5 as unable to generate more data.
        expected_streams_2 = self.expected_streams() - expected_streams_1 - {'questions', 'answers'}
        self.run_test(expected_streams_2, page_size=5)

    def run_test(self, expected_streams, page_size):

        streams_to_test = expected_streams
    
        self.PAGE_SIZE = page_size

        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_refresh_token)

        # Verify that there are catalogs found
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
         
                # Verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, -1)
                self.assertGreater(record_count_sync, page_size,
                                    msg="The number of records is not over the stream max limit")

                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                        for message in synced_records.get(stream).get('messages')
                                        if message.get('action') == 'upsert']

                primary_keys_list_1 = primary_keys_list[:page_size]
                primary_keys_list_2 = primary_keys_list[page_size:2*page_size]

                primary_keys_page_1 = set(primary_keys_list_1)
                primary_keys_page_2 = set(primary_keys_list_2)

                # Verify by primary keys that data is unique for page
                self.assertTrue(
                    primary_keys_page_1.isdisjoint(primary_keys_page_2))

                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / self.PAGE_SIZE)
                page_size = self.PAGE_SIZE
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # don't compare the page to itself

                            self.assertTrue(
                                current_page.isdisjoint(other_page), msg=f'other_page_primary_keys={other_page}'
                            )
