from copy import deepcopy
import json
import pendulum
from datetime import datetime
import singer
from singer import bookmarks


LOGGER = singer.get_logger()

MAX_METRIC_JOB_TIME = 1800
METRIC_JOB_POLL_SLEEP = 1
FORM_STREAMS = ['landings', 'answers'] #streams that get sync'd in sync_forms
MAX_RESPONSES_PAGE_SIZE = 1000
FORMS_PAGE_SIZE = 100

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

    # For the each child, write the bookmark if it is selected.
    for child in stream_obj.children:
        write_bookmarks(child, selected_streams, form_id, bookmark_value, state)

def write_forms_state(state, stream_name, form_id, value):
    if isinstance(value, dict):
        singer.write_bookmark(state, stream_name, form_id, value)
    else:
        singer.write_bookmark(state, stream_name, form_id, value.to_datetime_string())
    singer.write_state()

class Stream:
    tap_stream_id = None
    replication_method = None
    replication_keys = []
    key_properties = []
    endpoint = None
    filter_param = False
    children = []
    headers = {}
    params = {}
    parent = None
    data_key = None
    child_data_key = None

    def add_fields_at_1st_level(self, record, additional_data={}):
        pass

    def sync_child_stream(self, record, catalogs, state, selected_stream_ids, form_id, start_date, max_bookmark):

        for child in self.children:
            child_obj = STREAMS[child]()
            child_bookmark = get_bookmark(state, child_obj.tap_stream_id, form_id, self.replication_keys[0], start_date)

            if child in selected_stream_ids and record[child_obj.replication_keys[0]] >= child_bookmark:
                child_catalog = get_schema(catalogs, child)
                for rec in record[self.child_data_key]:
                    child_obj.add_fields_at_1st_level(rec, record)
                write_records(child_catalog, child_obj.tap_stream_id, record[self.child_data_key])
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

                    # Write selected child records
                    if self.children and self.child_data_key in record:
                        max_bookmark =  self.sync_child_stream(record, catalogs, state, selected_stream_ids,form_id, start_date, max_bookmark)

        # Update bookmark at the end
        write_bookmarks(self.tap_stream_id, selected_stream_ids, form_id, max_bookmark, state)
        return max_bookmark

    def sync_obj(self, client, state, catalogs, form_id,
                 start_date, selected_stream_ids):
        full_url = client.build_url(self.endpoint).format(form_id)
        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        bookmark = get_bookmark(state, self.tap_stream_id, form_id, self.replication_keys[0], start_date)
        min_bookmark_value = get_min_bookmark(self.tap_stream_id, selected_stream_ids,
                                              current_time, start_date, state, form_id, self.replication_keys[0])
        max_bookmark = bookmark
        page_count = 2
        params = {**self.params}
        params['since'] = int(pendulum.parse(min_bookmark_value).timestamp())

        while page_count > 1:
            response = client.request(full_url, params)
            records = response[self.data_key]
            max_bookmark = self.write_records(records, catalogs, selected_stream_ids,
                                                    form_id, max_bookmark, state, start_date)
            page_count = response.get('page_count', 0)
            if records:
                params['before'] = records[-1].get('token')

        write_bookmarks(self.tap_stream_id, selected_stream_ids, form_id, max_bookmark, state)
        singer.write_state(state)

class FullTableStream(Stream):
    endpoint = 'forms/{}'

    replication_method = 'FULL_TABLE'

    def sync_obj(self, client, state, catalogs, form,
                 start_date, selected_stream_ids):
        full_url = client.build_url(self.endpoint).format(form)
        stream_catalog = get_schema(catalogs, self.tap_stream_id)
        response = client.request(full_url, params=self.params)

        for record in response[self.data_key]:
            self.add_fields_at_1st_level(record,{"form_id": form})

        write_records(stream_catalog, self.tap_stream_id, response[self.data_key])

class Forms(IncrementalStream):
    tap_stream_id = 'forms'
    key_properties = ['id']
    endpoint='forms'
    replication_method = 'INCREMENTAL'
    replication_keys = ['last_updated_at']
    data_key = 'items'
    params = {'page_size': FORMS_PAGE_SIZE}

    def get_forms(self, client):
        full_url = client.build_url(self.endpoint)
        page = 1
        paginate = True
        params = {**self.params}
        while paginate:
            params['page'] = page
            response = client.request(full_url, params=params)
            page_count = response.get('page_count')
            paginate = page_count > page
            page += 1
            yield response.get(self.data_key)

    def sync_obj(self, client, state, catalogs,
                 start_date, selected_stream_ids):
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
    params = {
                'since': '',
                'page_size': MAX_RESPONSES_PAGE_SIZE,
            }
    data_key = 'fields'

    def add_fields_at_1st_level(self, record, additional_data={}):

        record.update({
            "form_id": additional_data['form_id'],
            "question_id": record['id']
            })

class Landings(IncrementalStream):
    tap_stream_id = 'landings'
    replication_keys = ['landed_at']
    key_properties = ['landing_id']
    endpoint = 'forms/{}/responses'
    children = ['answers']
    headers = None
    params = {
                'since': '',
                'page_size': MAX_RESPONSES_PAGE_SIZE,
                'completed': True
            }
    data_key = 'items'
    child_data_key = 'answers'

    def add_fields_at_1st_level(self, record, additional_data={}):
        record.update({
                "_sdc_form_id": additional_data["_sdc_form_id"],
                "user_agent": record["metadata"]["user_agent"],
                "platform": record["metadata"]["platform"],
                "referer": record["metadata"]["referer"],
                "network_id": record["metadata"]["network_id"],
                "browser": record["metadata"]["browser"],
                "hidden": json.dumps(record["hidden"]) if "hidden" in record else ""
        })

    def sync_obj(self, client, state, catalogs, form_id, start_date, selected_stream_ids):
        old_state = deepcopy(state)
        super().sync_obj(client, state, catalogs, form_id, start_date, selected_stream_ids)

        if client.fetch_uncompleted_forms:
            full_url = client.build_url(self.endpoint).format(form_id)
            bookmark = get_bookmark(old_state, self.tap_stream_id, form_id, self.replication_keys[0], start_date)

            max_bookmark = bookmark
            page_count = 2
            params = {**self.params}
            params['since'] = int(pendulum.parse(bookmark).timestamp())
            params['completed'] = False

            while page_count > 1:
                response = client.request(full_url, params)
                records = response[self.data_key]
                max_bookmark = self.write_records(records, catalogs, selected_stream_ids,
                                                        form_id, max_bookmark, old_state, start_date)
                page_count = response.get('page_count', 0)
                if records:
                    params['before'] = records[-1].get('token')

            max_bookmark = max(max_bookmark, get_bookmark(state, self.tap_stream_id, form_id, self.replication_keys[0], start_date))
            write_bookmarks(self.tap_stream_id, selected_stream_ids, form_id, max_bookmark, state)
            singer.write_state(state)

class Answers(IncrementalStream):
    tap_stream_id = 'answers'
    replication_keys = ['landed_at']
    key_properties = ['landing_id', 'question_id']
    parent = 'landings'
    data_key = 'answers'

    def add_fields_at_1st_level(self, record, additional_data = {}):
        data_type = record.get('type')

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
            "landed_at": additional_data.get('landed_at'),
            "answer": answer_value
        })

STREAMS = {
    "forms": Forms,
    "questions": Questions,
    "landings": Landings,
    "answers": Answers
}
