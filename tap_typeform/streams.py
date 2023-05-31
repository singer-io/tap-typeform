import json
import pendulum
from datetime import datetime
import singer
from singer import bookmarks


LOGGER = singer.get_logger()


def write_records(catalog_entry, tap_stream_id, records):
    extraction_time = singer.utils.now()
    stream_metadata = singer.metadata.to_map(catalog_entry['metadata'])
    stream_schema = catalog_entry['schema']
    with singer.metrics.record_counter(tap_stream_id) as counter:
        with singer.Transformer() as transformer:
            for rec in records:
                rec = transformer.transform(rec, stream_schema, stream_metadata)
                singer.write_record(tap_stream_id, rec, time_extracted=extraction_time)
        counter.increment(len(records))

def get_bookmark(state, stream_name, form_id, bookmark_key, start_date):
    """
    Return bookmark value if available in the state otherwise return start date
    """
    if form_id:
        return bookmarks.get_bookmark(state, stream_name, form_id, {}).get(bookmark_key, start_date)
    return bookmarks.get_bookmark(state, stream_name, bookmark_key,start_date)

def get_min_bookmark(stream, selected_streams, bookmark, start_date, state, form_id, bookmark_key):
    """
    Get the minimum bookmark from the parent and its corresponding child bookmarks.
    """

    stream_obj = STREAMS[stream]()
    min_bookmark = bookmark
    if stream in selected_streams:
        min_bookmark = min(min_bookmark, get_bookmark(state, stream, form_id, bookmark_key, start_date))

    for child in filter(lambda x: x in selected_streams, stream_obj.children):
        min_bookmark = min(min_bookmark, get_min_bookmark(child, selected_streams, bookmark, start_date, state, form_id, bookmark_key))

    return min_bookmark

def get_schema(catalog, stream_id):
    """
    Return catalog of the specified stream.
    """
    stream_catalog = [cat for cat in catalog if cat['tap_stream_id'] == stream_id ][0]
    return stream_catalog

def write_bookmarks(stream, selected_streams, form_id, bookmark_value, state):
    stream_obj = STREAMS[stream]()
    # If the stream is selected, write the bookmark.
    if stream in selected_streams:
        if form_id:
            singer.write_bookmark(state, stream_obj.tap_stream_id, form_id, {stream_obj.replication_keys[0]: bookmark_value})
        else:
            singer.write_bookmark(state, stream_obj.tap_stream_id, stream_obj.replication_keys[0], bookmark_value)

    # For each child, write the bookmark if it is selected.
    for child in stream_obj.children:
        write_bookmarks(child, selected_streams, form_id, bookmark_value, state)

class Stream:
    """
    Base class representing tap-typeform streams.
    """
    tap_stream_id = None
    replication_method = None
    replication_keys = None
    key_properties = []
    endpoint = None
    filter_param = False
    children = []
    headers = {}
    params = {}
    parent = None
    data_key = None
    child_data_key = None
    records_count = {}

    def add_fields_at_1st_level(self, record, additional_data={}):
        pass

    def sync_child_stream(self, record, catalogs, state, selected_stream_ids, form_id, start_date, max_bookmark):

        for child in self.children:
            child_obj = STREAMS[child]()
            child_bookmark = get_bookmark(state, child_obj.tap_stream_id, form_id, self.replication_keys[0], start_date)

            if child in selected_stream_ids and record[child_obj.replication_keys[0]] >= child_bookmark and record[self.child_data_key]:
                child_catalog = get_schema(catalogs, child)
                for rec in record[self.child_data_key]:
                    child_obj.add_fields_at_1st_level(rec, {**record, "_sdc_form_id": form_id})
                write_records(child_catalog, child_obj.tap_stream_id, record[self.child_data_key])
                self.records_count[child_obj.tap_stream_id] += len(record[self.child_data_key])
                max_bookmark = max(max_bookmark, record[child_obj.replication_keys[0]])
        return max_bookmark

class IncrementalStream(Stream):

    replication_method = 'INCREMENTAL'

    def write_records(self, records, catalogs, selected_stream_ids,
                        form_id, max_bookmark, state, start_date):
        stream_catalog = get_schema(catalogs, self.tap_stream_id)
        bookmark = get_bookmark(state, self.tap_stream_id, form_id, self.replication_keys[0], start_date)

        with singer.metrics.record_counter(self.tap_stream_id) as counter: 
            with singer.Transformer() as transformer:
                extraction_time = singer.utils.now()
                stream_metadata = singer.metadata.to_map(stream_catalog['metadata'])

                for record in records:
                    self.add_fields_at_1st_level(record, {"_sdc_form_id": form_id})
                    if self.tap_stream_id in selected_stream_ids and record[self.replication_keys[0]] >= bookmark:
                        rec = transformer.transform(record, stream_catalog['schema'], stream_metadata)
                        singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
                        max_bookmark = max(max_bookmark, record[self.replication_keys[0]])
                        counter.increment(1)
                        self.records_count[self.tap_stream_id] += 1

                    # Write selected child records
                    if self.children and self.child_data_key in record:
                        max_bookmark =  self.sync_child_stream(record, catalogs, state, selected_stream_ids,form_id, start_date, max_bookmark)

        return max_bookmark

    def sync_obj(self, client, state, catalogs, form_id,
                    start_date, selected_stream_ids, records_count):
        self.records_count = records_count
        full_url = client.build_url(self.endpoint).format(form_id)
        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        bookmark = get_bookmark(state, self.tap_stream_id, form_id, self.replication_keys[0], start_date)

        # Get minimum bookmark of child and parent streams.
        min_bookmark_value = get_min_bookmark(self.tap_stream_id, selected_stream_ids,
                                                current_time, start_date, state, form_id, self.replication_keys[0])
        LOGGER.info('Syncing  stream {} - form: {} start_date: {}'.format(
                    self.tap_stream_id, form_id, pendulum.parse(min_bookmark_value).strftime("%Y-%m-%d %H:%M")))
        max_bookmark = bookmark
        page_count = 2
        params = {**self.params, "page_size": client.page_size}
        params['since'] = int(pendulum.parse(min_bookmark_value).timestamp())

        while page_count > 1:
            response = client.request(full_url, params)
            records = response[self.data_key]
            max_bookmark = self.write_records(records, catalogs, selected_stream_ids,
                                                    form_id, max_bookmark, state, start_date)
            page_count = response.get('page_count', 0)

            # To get the next page, set param field
            if records:
                params['before'] = records[-1].get('token')

        write_bookmarks(self.tap_stream_id, selected_stream_ids, form_id, max_bookmark, state)
        singer.write_state(state)

class FullTableStream(Stream):
    endpoint = 'forms/{}'

    replication_method = 'FULL_TABLE'

    def sync_obj(self, client, state, catalogs, form_id,
                    start_date, selected_stream_ids, records_count):
        LOGGER.info('Syncing  stream {} - form: {}'.format(
                    self.tap_stream_id, form_id))
        self.records_count = records_count
        full_url = client.build_url(self.endpoint).format(form_id)
        stream_catalog = get_schema(catalogs, self.tap_stream_id)
        response = client.request(full_url, params=self.params)

        if self.data_key not in response:
            LOGGER.info('There are no questions associated with form {}'.format(form_id))
            return

        for record in response[self.data_key]:
            self.add_fields_at_1st_level(record, {"form_id": form_id})

        write_records(stream_catalog, self.tap_stream_id, response[self.data_key])
        self.records_count[self.tap_stream_id] += len(response[self.data_key])

class Forms(IncrementalStream):
    tap_stream_id = 'forms'
    key_properties = ['id']
    endpoint='forms'
    replication_method = 'INCREMENTAL'
    replication_keys = ['last_updated_at']
    data_key = 'items'
    params = {
        'sort_by': 'last_updated_at',
        'order_by': 'asc'
    }

    def get_forms(self, client):
        full_url = client.build_url(self.endpoint)
        page = 1
        paginate = True
        params = {**self.params, "page_size": client.form_page_size}
        while paginate:
            params['page'] = page
            response = client.request(full_url, params=params)
            page_count = response.get('page_count')
            paginate = page_count > page
            page += 1
            yield response.get(self.data_key)

    def sync_obj(self, client, state, catalogs,
                    start_date, selected_stream_ids, records_count):
        self.records_count = records_count
        bookmark = state.get('bookmarks',{}).get(self.tap_stream_id,{}).get(self.replication_keys[0], start_date)
        max_bookmark = bookmark

        for records in self.get_forms(client):
            
            max_bookmark = self.write_records(records, catalogs, selected_stream_ids,
                        None, max_bookmark, state, start_date)
            write_bookmarks(self.tap_stream_id, selected_stream_ids, None, max_bookmark, state)

        singer.write_state(state)

class Questions(FullTableStream):
    tap_stream_id = 'questions'
    key_properties = ['form_id', 'question_id']
    endpoint = 'forms/{}'
    data_key = 'fields'

    def fetch_sub_questions(self, row):
        '''This function fetches records for each sub_question in a question group and returns a list of fetched sub_questions'''
        sub_questions = [] #Creating blank list to accommodate each sub-question's records

        #Appending each sub-question to the list
        for question in row['properties'].get('fields',[]):
            sub_questions.append({
                "question_id": question['id'],
                "title": question['title'],
                "ref": question['ref']
            })

        return sub_questions

    def add_fields_at_1st_level(self, record, additional_data={}):
        """
        Add additional data and nested fields to top level
        """
        sub_questions ={} #Creating a blank dictionary to store records of sub_questions,if any

        #If type of question is group, i.e. it has sub_questions, then fetch those sub_questions
        if record.get('type') == 'group':
            sub_questions['sub_questions'] = self.fetch_sub_questions(record)

        #If sub_questions are fetched then add those in this field and display the same, else don't display this field
        record.update({
            "form_id": additional_data['form_id'],
            "question_id": record['id']
            }|sub_questions)

class SubmittedLandings(IncrementalStream):
    tap_stream_id = 'submitted_landings'
    replication_keys = ['submitted_at']
    key_properties = ['landing_id']
    endpoint = 'forms/{}/responses'
    children = ['answers']
    params = {
                'since': '',
                'page_size': '',
                'completed': True
            }
    data_key = 'items'
    child_data_key = 'answers'

    def add_fields_at_1st_level(self, record, additional_data={}):
        """
        Add additional data and nested fields to top level
        """
        record.update({
                "tags": record.get("tags"),
                "_sdc_form_id": additional_data["_sdc_form_id"],
                "user_agent": record["metadata"]["user_agent"],
                "platform": record["metadata"]["platform"],
                "referer": record["metadata"]["referer"],
                "network_id": record["metadata"]["network_id"],
                "browser": record["metadata"]["browser"],
                "hidden": json.dumps(record["hidden"]) if "hidden" in record else ""
        })

class UnsubmittedLandings(IncrementalStream):
    tap_stream_id = 'unsubmitted_landings'
    replication_keys = ['landed_at']
    key_properties = ['landing_id']
    endpoint = 'forms/{}/responses'
    params = {
                'since': '',
                'page_size': '',
                'completed': False
            }
    data_key = 'items'

    def add_fields_at_1st_level(self, record, additional_data={}):
        """
        Add additional data and nested fields to top level
        """
        record.update({
                "_sdc_form_id": additional_data["_sdc_form_id"],
                "user_agent": record["metadata"]["user_agent"],
                "platform": record["metadata"]["platform"],
                "referer": record["metadata"]["referer"],
                "network_id": record["metadata"]["network_id"],
                "browser": record["metadata"]["browser"],
        })


class Answers(IncrementalStream):
    tap_stream_id = 'answers'
    replication_keys = ['submitted_at']
    key_properties = ['landing_id', 'question_id']
    parent = 'submitted_landings'
    data_key = 'answers'

    def add_fields_at_1st_level(self, record, additional_data = {}):
        """
        Add additional data and nested fields to top level
        """
        data_type = record.get('type')

        # Transform data_value according to data_type
        if data_type in ['choice', 'choices', 'payment']:
            answer_value = json.dumps(record.get(data_type))
        elif data_type in ['number', 'boolean']:
            answer_value = str(record.get(data_type))
        else:
            answer_value = record.get(data_type)

        record.update({
            "_sdc_form_id": additional_data['_sdc_form_id'],
            "landing_id": additional_data.get('landing_id'),
            "question_id": record.get('field',{}).get('id'),
            "type": record.get('field',{}).get('type'),
            "ref": record.get('field',{}).get('ref'),
            "data_type": data_type,
            "submitted_at": additional_data.get('submitted_at'),
            "answer": answer_value
        })

STREAMS = {
    "forms": Forms,
    "questions": Questions,
    "submitted_landings": SubmittedLandings,
    "unsubmitted_landings": UnsubmittedLandings,
    "answers": Answers,
}
