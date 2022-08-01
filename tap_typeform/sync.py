import singer
import pendulum
from tap_typeform.streams import STREAMS

LOGGER = singer.get_logger()

def _forms_to_list(config, keyword='forms'):
    """
    Splits entries into a list and strips out surrounding blank spaces.
    """
    return set(map(str.strip, config.get(keyword).split(',')))


def write_schemas(stream_id, catalog, selected_streams):
    """
    Write the schemas for each stream.
    """
    stream_obj = STREAMS[stream_id]()

    if stream_id in selected_streams:
        # Get catalog object for a particular stream.
        stream = [cat for cat in catalog['streams'] if cat['tap_stream_id'] == stream_id ][0]
        singer.write_schema(stream_id, stream['schema'], stream['key_properties'])

    for child in stream_obj.children:
        write_schemas(child, catalog, selected_streams)

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        for entry in stream_metadata:
            # Stream metadata will have an empty breadcrumb
            if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                selected_streams.append(stream['tap_stream_id'])
    return selected_streams

def get_stream_to_sync(selected_streams):
    """
    Get the streams for which the sync function should be called(the parent in case of selected child streams).
    """
    streams_to_sync = []
    for stream_name, stream_obj in STREAMS.items():
        if (stream_name in selected_streams) or any(child in selected_streams for child in stream_obj.children):
            streams_to_sync.append(stream_name)
    return streams_to_sync

def sync(client, config, state, catalog):
    """
    Sync selected streams.
    """

    # Get selected streams, make sure stream dependencies are met
    selected_streams = get_selected_streams(catalog)
    streams_to_sync = get_stream_to_sync(selected_streams)
    LOGGER.info("Selected Streams: %s", selected_streams)
    LOGGER.info("Syncing Streams: %s", streams_to_sync)

    # Initializing a dictionary to keep track of record count by streams
    records_count = {stream:0 for stream in STREAMS.keys()}

    singer.write_state(state)
    for stream in streams_to_sync:
        stream_obj = STREAMS[stream]()

        # Calling `forms` sync object separately as it does not take called once
        # independent of form ids
        if stream == 'forms' and stream in selected_streams:
            write_schemas(stream, catalog, selected_streams)

            stream_obj.sync_obj(client, state, catalog['streams'], config["start_date"],
                                selected_streams, records_count)
        elif not stream_obj.parent:
            write_schemas(stream, catalog, selected_streams)

            for form in _forms_to_list(config):

                stream_obj.sync_obj(client, state, catalog['streams'], form, config["start_date"],
                                    selected_streams, records_count)

    for stream_name, stream_count in records_count.items():
        LOGGER.info('%s: %d', stream_name, stream_count)
