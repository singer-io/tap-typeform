#!/usr/bin/env python3
import singer
from singer import utils
from singer.catalog import Catalog, metadata_module as metadata
from tap_typeform import streams
from tap_typeform.context import Context
from tap_typeform.http import Client
from tap_typeform import schemas

REQUIRED_CONFIG_KEYS = ["token", "forms", "incremental_range"]

LOGGER = singer.get_logger()

#def check_authorization(atx):
#    atx.client.get('/settings')
class FormMistmatchError(Exception):
    pass

class NoFormsProvidedError(Exception):
    pass


# Some taps do discovery dynamically where the catalog is read in from a
#  call to the api but with the typeform structure, we won't do that here
#  because it's always the same so we just pull it from file we never use
#  atx in here since the schema is from file but we would use it if we
#  pulled schema from the API def discover(atx):
def discover():
    streams = []
    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
        #print("tap stream id=",tap_stream_id)
        key_properties = schemas.PK_FIELDS[tap_stream_id]
        schema = schemas.load_schema(tap_stream_id)
        replication_method = schemas.REPLICATION_METHODS[tap_stream_id].get("replication_method")
        replication_keys = schemas.REPLICATION_METHODS[tap_stream_id].get("replication_keys")
        meta = metadata.get_standard_metadata(schema=schema,
                                              key_properties=key_properties,
                                              replication_method=replication_method,
                                              valid_replication_keys=replication_keys)

        meta = metadata.to_map(meta)

        if replication_keys:
            meta = metadata.write(meta, ('properties', replication_keys[0]), 'inclusion', 'automatic')

        meta = metadata.to_list(meta)

        streams.append({
            'stream': tap_stream_id,
            'tap_stream_id': tap_stream_id,
            'key_properties': key_properties,
            'schema': schema,
            'metadata': meta,
            'replication_method': replication_method,
            'replication_key': replication_keys[0] if replication_keys else None
        })
    return Catalog.from_dict({'streams': streams})


# this is already defined in schemas.py though w/o dependencies.  do we keep this for the sync?
def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def sync(atx):

    # write schemas for selected streams\
    for stream in atx.catalog.streams:
        if stream.tap_stream_id in atx.selected_stream_ids:
            schemas.load_and_write_schema(stream.tap_stream_id)

    # since there is only one set of schemas for all forms, they will always be selected
    streams.sync_forms(atx)

    LOGGER.info('--------------------')
    for stream_name, stream_count in atx.counts.items():
        LOGGER.info('%s: %d', stream_name, stream_count)
    LOGGER.info('--------------------')


def _compare_forms(config_forms, api_forms):
    return config_forms.difference(api_forms)


def _forms_to_list(config, keyword='forms'):
    """Splits entries into a list and strips out surrounding blank spaces"""
    return list(map(str.strip, config.get(keyword).split(',')))


def validate_form_ids(config):
    """Validate the form ids passed in the config"""
    client = Client(config)

    if not config.get('forms'):
        LOGGER.fatal("No forms were provided in config")
        raise NoFormsProvidedError

    config_forms = set(_forms_to_list(config))
    api_forms = {form.get('id') for form in client.get_forms()}

    mismatched_forms = _compare_forms(config_forms, api_forms)

    if len(mismatched_forms) > 0:
        LOGGER.fatal(f"FormMistmatchError: forms {mismatched_forms} not returned by API")
        raise FormMistmatchError


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    atx = Context(args.config, args.state)
    if args.discover:
        validate_form_ids(args.config)
        # the schema is static from file so we don't need to pass in atx for connection info.
        catalog = discover()
        catalog.dump()
    else:
        atx.catalog = args.catalog \
            if args.catalog else discover()
        sync(atx)

if __name__ == "__main__":
    main()
