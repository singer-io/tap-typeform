#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams
from .context import Context
from . import schemas

REQUIRED_CONFIG_KEYS = ["token", "forms", "incremental_range"]

LOGGER = singer.get_logger()

#def check_authorization(atx):
#    atx.client.get('/settings')


# Some taps do discovery dynamically where the catalog is read in from a
#  call to the api but with the typeform structure, we won't do that here
#  because it's always the same so we just pull it from file we never use
#  atx in here since the schema is from file but we would use it if we
#  pulled schema from the API def discover(atx):
def discover():
    catalog = Catalog([])
    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
        #print("tap stream id=",tap_stream_id)
        schema = Schema.from_dict(schemas.load_schema(tap_stream_id))
        metadata = []
        for field_name in schema.properties.keys():
            #print("field name=",field_name)
            if field_name in schemas.PK_FIELDS[tap_stream_id]:
                inclusion = 'automatic'
            else:
                inclusion = 'available'
            metadata.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', field_name]
            })
        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata=metadata
        ))
    return catalog


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
    schemas.load_and_write_schema('questions')
    schemas.load_and_write_schema('landings')
    schemas.load_and_write_schema('answers')

    # since there is only one set of schemas for all forms, they will always be selected
    streams.sync_forms(atx)


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    atx = Context(args.config, args.state)
    if args.discover:
        # the schema is static from file so we don't need to pass in atx for connection info.
        catalog = discover()
        json.dump(catalog.to_dict(), sys.stdout)
    else:
        atx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover()
        sync(atx)

if __name__ == "__main__":
    main()
