from singer.catalog import Catalog, metadata_module as metadata
from tap_typeform import schema


def discover():
    streams = []
    for tap_stream_id in schema.STATIC_SCHEMA_STREAM_IDS:
        key_properties = schema.PK_FIELDS[tap_stream_id]
        stream_schema = schema.load_schema(tap_stream_id)
        replication_method = schema.REPLICATION_METHODS[tap_stream_id].get("replication_method")
        replication_keys = schema.REPLICATION_METHODS[tap_stream_id].get("replication_keys")
        meta = metadata.get_standard_metadata(schema=stream_schema,
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
            'schema': stream_schema,
            'metadata': meta,
        })
    return Catalog.from_dict({'streams': streams})
