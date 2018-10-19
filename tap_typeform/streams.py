import time
import datetime
import json

import pendulum
import singer
from singer.bookmarks import write_bookmark, reset_stream
from ratelimit import limits, sleep_and_retry, RateLimitException
from backoff import on_exception, expo, constant

from .http import MetricsRateLimitException

LOGGER = singer.get_logger()

MAX_METRIC_JOB_TIME = 1800
METRIC_JOB_POLL_SLEEP = 1

def count(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
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

@on_exception(constant, MetricsRateLimitException, max_tries=5, interval=60)
@on_exception(expo, RateLimitException, max_tries=5)
@sleep_and_retry
@limits(calls=1, period=6) # 5 seconds needed to be padded by 1 second to work
def get_form_definition(atx, form_id):
    return atx.client.get(form_id)

@on_exception(constant, MetricsRateLimitException, max_tries=5, interval=60)
@on_exception(expo, RateLimitException, max_tries=5)
@sleep_and_retry
@limits(calls=1, period=6) # 5 seconds needed to be padded by 1 second to work
def get_form(atx, form_id, start_date, end_date):
    LOGGER.info('Forms query - form: {} start_date: {} end_date: {} '.format(
        form_id,
        start_date,
        end_date))
    # the api limits responses to a max of 1000 per call
    # the api doesn't have a means of paging through responses if the number is greater than 1000,
    # so since the order of data retrieved is by submitted_at we have 
    # to take the last submitted_at date and use it to cycle through
    return atx.client.get(form_id, params={'since': start_date, 'until': end_date, 'page_size': 1000})

def sync_form_definition(atx, form_id):
    with singer.metrics.job_timer('form definition '+form_id):
        start = time.monotonic()
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            response = get_form_definition(atx, form_id)
            data = response['fields']
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

    write_records('questions', definition_data_rows)


def sync_form(atx, form_id, start_date, end_date):
    with singer.metrics.job_timer('form '+form_id):
        start = time.monotonic()
        # we've really moved this functionality to the request in the http script
        #so we don't expect that this will actually have to run mult times
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            response = get_form(atx, form_id, start_date, end_date)
            data = response['items']
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    landings_data_rows = []
    answers_data_rows = []

    max_submitted_dt = ''

    for row in data:
        if 'hidden' not in row:
            hidden = ''
        else:
            hidden = json.dumps(row['hidden'])

        # the schema here reflects what we saw through testing
        # the typeform documentation is subtly inaccurate
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

        max_submitted_dt = row['submitted_at']

        if 'answers' in row:
            for answer in row['answers']:
                data_type = answer['type']

                if data_type in ['choice', 'choices', 'payment']:
                    answer_value = json.dumps(answer[data_type])
                elif data_type in ['number', 'boolean']:
                    answer_value = str(answer[data_type])
                else:
                    answer_value = answer[data_type]

                answers_data_rows.append({
                    "landing_id": row['landing_id'],
                    "question_id": answer['field']['id'],
                    "type": answer['field']['type'],
                    "ref": answer['field']['ref'],
                    "data_type": data_type,
                    "answer": answer_value
                })

    write_records('landings', landings_data_rows)
    write_records('answers', answers_data_rows)

    return [response['total_items'], max_submitted_dt]


def write_forms_state(atx, form, date_to_resume):
    write_bookmark(atx.state, form, 'date_to_resume', date_to_resume.to_datetime_string())
    atx.write_state()

def sync_forms(atx):
    incremental_range = atx.config.get('incremental_range')

    for form_id in atx.config.get('forms').split(','):
        bookmark = atx.state.get('bookmarks', {}).get(form_id, {})

        LOGGER.info('form: {} '.format(form_id))

        # pull back the form question details
        sync_form_definition(atx, form_id)

        # start_date is defaulted in the config file 2018-01-01
        # if there's no default date and it gets set to now, then start_date will have to be
        #   set to the prior business day/hour before we can use it.
        now = datetime.datetime.now()
        if incremental_range == "daily":
            s_d = now.replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = pendulum.parse(atx.config.get('start_date', s_d + datetime.timedelta(days=-1, hours=0)))
        elif incremental_range == "hourly":
            s_d = now.replace(minute=0, second=0, microsecond=0)
            start_date = pendulum.parse(atx.config.get('start_date', s_d + datetime.timedelta(days=0, hours=-1)))
        LOGGER.info('start_date: {} '.format(start_date))


        # end date is not usually specified in the config file by default so end_date is now.
        # if end date is now, we will have to truncate them
        # to the nearest day/hour before we can use it.
        if incremental_range == "daily":
            e_d = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            end_date = pendulum.parse(atx.config.get('end_date', e_d))
        elif incremental_range == "hourly":
            e_d = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            end_date = pendulum.parse(atx.config.get('end_date', e_d))
        LOGGER.info('end_date: {} '.format(end_date))

        # if the state file has a date_to_resume, we use it as it is.
        # if it doesn't exist, we overwrite by start date
        s_d = start_date.strftime("%Y-%m-%d %H:%M:%S")
        last_date = pendulum.parse(bookmark.get('date_to_resume', s_d))
        LOGGER.info('last_date: {} '.format(last_date))


        # no real reason to assign this other than the naming
        # makes better sense once we go into the loop
        current_date = last_date

        while current_date <= end_date:
            if incremental_range == "daily":
                next_date = current_date + datetime.timedelta(days=1, hours=0)
            elif incremental_range == "hourly":
                next_date = current_date + datetime.timedelta(days=0, hours=1)

            ut_current_date = int(current_date.timestamp())
            LOGGER.info('ut_current_date: {} '.format(ut_current_date))
            ut_next_date = int(next_date.timestamp())
            LOGGER.info('ut_next_date: {} '.format(ut_next_date))
            [responses, max_submitted_at] = sync_form(atx, form_id, ut_current_date, ut_next_date)
            # if the max responses were returned, we have to make the call again
            # going to increment the max_submitted_at by 1 second so we don't get dupes,
            # but this also might cause some minor loss of data.
            # there's no ideal scenario here since the API has no other way than using
            # time ranges to step through data.
            while responses == 1000:
                interim_next_date = pendulum.parse(max_submitted_at) + datetime.timedelta(seconds=1)
                ut_interim_next_date = int(interim_next_date.timestamp())
                write_forms_state(atx, form_id, interim_next_date)
                [responses, max_submitted_at] = sync_form(atx, form_id, ut_interim_next_date, ut_next_date)

            # if the prior sync is successful it will write the date_to_resume bookmark
            write_forms_state(atx, form_id, next_date)
            current_date = next_date

        reset_stream(atx.state, 'questions')
        reset_stream(atx.state, 'landings')
        reset_stream(atx.state, 'answers')
