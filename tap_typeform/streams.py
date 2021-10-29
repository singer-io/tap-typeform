import json
import time

import pendulum
import pytz
import singer
from singer.bookmarks import write_bookmark

from tap_typeform import schemas

LOGGER = singer.get_logger()

MAX_METRIC_JOB_TIME = 1800
METRIC_JOB_POLL_SLEEP = 1
FORM_STREAMS = ['landings', 'answers'] #streams that get sync'd in sync_forms
MAX_RESPONSES_PAGE_SIZE = 1000

def count(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(atx, tap_stream_id, records):
    extraction_time = singer.utils.now()
    catalog_entry = atx.get_catalog_entry(tap_stream_id)
    stream_metadata = singer.metadata.to_map(catalog_entry.metadata)
    stream_schema = catalog_entry.schema.to_dict()
    with singer.Transformer() as transformer:
        for rec in records:
            rec = transformer.transform(rec, stream_schema, stream_metadata)
            singer.write_record(tap_stream_id, rec, time_extracted=extraction_time)
        atx.counts[tap_stream_id] += len(records)
    count(tap_stream_id, records)

def get_date_and_integer_fields(stream):
    date_fields = []
    integer_fields = []
    for prop, json_schema in stream.schema.properties.items():
        _type = json_schema.type
        if isinstance(_type, list) and 'integer' in _type or \
            _type == 'integer':
            integer_fields.append(prop)
        elif json_schema.format == 'date-time':
            date_fields.append(prop)
    return date_fields, integer_fields

def base_transform(date_fields, integer_fields, obj):
    new_obj = {}
    for field, value in obj.items():
        if value == '':
            value = None
        elif field in integer_fields and value is not None:
            value = int(value)
        elif field in date_fields and value is not None:
            value = pendulum.parse(value).isoformat()
        new_obj[field] = value
    return new_obj

def select_fields(mdata, obj):
    new_obj = {}
    for key, value in obj.items():
        field_metadata = mdata.get(('properties', key))
        if field_metadata and \
            (field_metadata.get('selected') is True or \
            field_metadata.get('inclusion') == 'automatic'):
            new_obj[key] = value
    return new_obj

def get_form_definition(atx, form_id):
    return atx.client.get_form_definition(form_id)

def get_form(atx, form_id, start_date, token=None, next_page=False):
    """Calls the API client get_form_responses() method with requisite params.

    The API limits responses to a max of 1000 per call, but it supports
        paging through responses using a `before` or `after` param.

    https://developer.typeform.com/responses/walkthroughs/#use-query-parameters-to-retrieve-specific-data

    By using a `sort` with `submitted_at` in ascending order in the initial call
        we can then use the `after` param that has the greatest `submitted_at`
        value in the initial response for the next call.

    :param atx: The Typeform API client.
    :param form_id: The form id to which responses are to be retrieved.
    :param start_date: Date to use in the `since` param of the API call.
    :param token: The token to use for the `after` param of the API call.
    :param next_page: Boolean if retreiving more records using the `after` param
        for paginating through responses.

    :return: API client response.
    """
    after = f'after token: {token} ' if token else ''
    LOGGER.info('Forms query - form: {} start_date: {} {}'.format(
        form_id,
        pendulum.from_timestamp(start_date).strftime("%Y-%m-%d %H:%M"),
        after))

    sort = None if next_page else 'submitted_at,asc' # sorting isn't suppored when using `after`

    return atx.client.get_form_responses(
        form_id,
        params={
            'since': start_date,
            'page_size': MAX_RESPONSES_PAGE_SIZE,
            'sort': sort,
            'after': token,
            })

def get_forms(atx):
    LOGGER.info('All forms query')
    return atx.client.get_forms()

def get_landings(atx, form_id):
    LOGGER.info('All landings query')

    page_count = 2
    sort = 'landed_at,asc'
    token = None

    while page_count > 1:
        response = atx.client.get_form_responses(
            form_id,
            params={
                'page_size': MAX_RESPONSES_PAGE_SIZE,
                'sort': sort,
                'after': token,
            })

        page_count = response.get('page_count', 1)
        sort = None
        items = response.get('items', [])
        if items:
            token = items[-1].get('token')


        yield from items


def sync_form_definition(atx, form_id):
    with singer.metrics.job_timer('form definition '+form_id):
        start = time.monotonic()
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            response = get_form_definition(atx, form_id)
            data = response.get('fields',[])
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    definition_data_rows = []

    # we only care about a few fields in the form definition
    # just those that give an analyst a reference to the submissions
    for row in data:
        definition_data_rows.append({
            "form_id": form_id,
            "question_id": row['id'],
            "title": row['title'],
            "ref": row['ref']
            })

    write_records(atx, 'questions', definition_data_rows)


def sync_form(atx, form_id, start_date, token=None, next_page=False):
    with singer.metrics.job_timer('form '+form_id):
        start = time.monotonic()
        # we've really moved this functionality to the request in the http script
        #so we don't expect that this will actually have to run mult times
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            response = get_form(atx, form_id, start_date, token, next_page)
            data = response['items']
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    answers_data_rows = []

    max_submitted_dt = pendulum.from_timestamp(start_date).isoformat()
    max_token = ''

    for row in data:

        max_submitted_dt = row['submitted_at']
        max_token = row['token']

        if row.get('answers') and 'answers' in atx.selected_stream_ids:
            for answer in row['answers']:
                data_type = answer.get('type')

                if data_type in ['choice', 'choices', 'payment']:
                    answer_value = json.dumps(answer.get(data_type))
                elif data_type in ['number', 'boolean']:
                    answer_value = str(answer.get(data_type))
                else:
                    answer_value = answer.get(data_type)

                answers_data_rows.append({
                    "landing_id": row.get('landing_id'),
                    "question_id": answer.get('field',{}).get('id'),
                    "type": answer.get('field',{}).get('type'),
                    "ref": answer.get('field',{}).get('ref'),
                    "data_type": data_type,
                    "landed_at": row.get("landed_at"),
                    "answer": answer_value
                })

    if 'answers' in atx.selected_stream_ids:
        write_records(atx, 'answers', answers_data_rows)

    return response.get('page_count', 0), max_submitted_dt, max_token


def write_forms_state(atx, stream_name, form_id, value):
    if isinstance(value, dict):
        write_bookmark(atx.state, stream_name, form_id, value)
    else:
        write_bookmark(atx.state, stream_name, form_id, value.to_datetime_string())
    atx.write_state()


def sync_latest_forms(atx):
    replication_key = 'last_updated_at'
    tap_id = 'forms'
    with singer.metrics.job_timer('all forms'):
        start = time.monotonic()
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            forms = get_forms(atx)
            if forms != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    # Using an older version of singer
    bookmark_date = singer.get_bookmark(atx.state, tap_id, replication_key) or atx.config['start_date']
    bookmark_datetime = singer.utils.strptime_to_utc(bookmark_date)
    max_datetime = bookmark_datetime

    records = []
    for form in forms:
        record_datetime = singer.utils.strptime_to_utc(form[replication_key])
        if record_datetime >= bookmark_datetime:
            records.append(form)
            max_datetime = max(record_datetime, max_datetime)

    write_records(atx, tap_id, records)
    bookmark_date = singer.utils.strftime(max_datetime)
    state = singer.write_bookmark(atx.state,
                                  tap_id,
                                  replication_key,
                                  bookmark_date)

    return state


def is_incremental(stream):
    return schemas.REPLICATION_METHODS[stream].get("replication_method") == "INCREMENTAL"


def get_replication_key(stream):
    if is_incremental(stream):
        return schemas.REPLICATION_METHODS[stream].get("replication_keys")[0]
    return None


def construct_bookmark(stream_name, value):
    replication_key = get_replication_key(stream_name)
    if replication_key:
        bookmark_value = {replication_key: value.to_datetime_string()}
    else:
        bookmark_value = value

    return bookmark_value

def get_bookmark_value(state, stream_name, key, default=None):
    # singer.get_bookmark(atx.state, stream_name, form_id, default=s_d)
    bookmark = singer.get_bookmark(state, stream_name, key, default=default)

    if isinstance(bookmark, dict):
        return list(bookmark.values())[0]

    return bookmark


def sync_landings(atx, form_id):
    response = get_landings(atx, form_id)

    landings_data_rows = []

    for row in response:
        if 'hidden' not in row:
            hidden = ''
        else:
            hidden = json.dumps(row['hidden'])

        # the schema here reflects what we saw through testing
        # the typeform documentation is subtly inaccurate
        if 'landings' in atx.selected_stream_ids:
            landings_data_rows.append({
                "landing_id": row['landing_id'],
                "token": row['token'],
                "landed_at": row['landed_at'],
                "submitted_at": row['submitted_at'],
                "user_agent": row['metadata']['user_agent'],
                "platform": row['metadata']['platform'],
                "referer": row['metadata']['referer'],
                "network_id": row['metadata']['network_id'],
                "browser": row['metadata']['browser'],
                "hidden": hidden
            })

    write_records(atx, 'landings', landings_data_rows)


def sync_forms(atx):
    for form_id in atx.config.get('forms').split(','):
        LOGGER.info('form: {} '.format(form_id))

        # pull back the form question details
        if 'questions' in atx.selected_stream_ids:
            sync_form_definition(atx, form_id)

        if 'landings' in atx.selected_stream_ids:
            sync_landings(atx, form_id)

        should_sync_forms = False
        for stream_name in FORM_STREAMS:
            should_sync_forms = should_sync_forms or (stream_name in atx.selected_stream_ids)
        if not should_sync_forms:
            continue

        # start_date is defaulted in the config file 2018-01-01
        # if there's no default date and it gets set to now
        now_parsed = pendulum.now(pytz.utc)
        s_d = now_parsed.replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = pendulum.parse(atx.config.get('start_date', s_d.isoformat()))

        # if the state file has a date_to_resume, we use it as it is.
        # if it doesn't exist, we overwrite by start date
        s_d = start_date.strftime("%Y-%m-%d %H:%M:%S")
        last_date = pendulum.parse(get_bookmark_value(atx.state, stream_name, form_id, default=s_d))
        LOGGER.info('last_date: {} '.format(last_date))

        # no real reason to assign this other than the naming
        # makes better sense once we go into the loop
        current_date = last_date

        page_count = 0

        ut_current_date = int(current_date.timestamp())
        LOGGER.info('ut_current_date: {} '.format(ut_current_date))

        page_count, max_submitted_at, max_token = sync_form(atx, form_id, ut_current_date)

        parsed_max_submitted_at = pendulum.parse(max_submitted_at)
        bookmark_value = construct_bookmark(stream_name, parsed_max_submitted_at)

        # if the max responses were returned, we have to make the call again
        # using the max_token from the initial call.
        while page_count > 1:
            page_count, max_submitted_at, max_token = sync_form(atx,
                                                                form_id,
                                                                ut_current_date,
                                                                token=max_token,
                                                                next_page=True)

            parsed_max_submitted_at = pendulum.parse(max_submitted_at)
            bookmark_value = construct_bookmark(stream_name, parsed_max_submitted_at)
            write_forms_state(atx, stream_name, form_id, bookmark_value)

        # check if bookmark is in the past
        updated_bookmark_value = max(parsed_max_submitted_at, now_parsed)
        bookmark_value = construct_bookmark(stream_name, updated_bookmark_value)

        # if the prior sync is successful it will write the date_to_resume bookmark
        write_forms_state(atx, stream_name, form_id, bookmark_value)

    if 'forms' in atx.selected_stream_ids:
        state = sync_latest_forms(atx)
        singer.write_state(state)
