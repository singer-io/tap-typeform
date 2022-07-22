import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_typeform.schema import get_schemas

LOGGER = singer.get_logger()

def discover():
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    """
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error('stream_name: %s', stream_name)
            LOGGER.error('type schema_dict: %s', type(schema_dict))
            raise err

        key_properties = mdata[0]['metadata'].get('table-key-properties')
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties= key_properties,
            schema=schema,
            metadata=mdata
        ))

    return catalog
