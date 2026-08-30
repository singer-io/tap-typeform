import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_typeform.schema import get_schemas
from tap_typeform.streams import STREAMS
from tap_typeform.client import TypeformForbiddenError

LOGGER = singer.get_logger()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    to_exclude = []
    for name, stream_cls in list(STREAMS.items()):
        if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name, stream_cls.parent,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)
            to_exclude.append(name)
    return to_exclude


def _apply_access_checks(client, schemas: dict, field_metadata: dict, form_id=None) -> None:
    """
    Probe each stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.
    Note: check_access() always returns True for child streams, so this loop
    effectively identifies only inaccessible parent streams by design.
    Child stream removal is handled separately by _prune_inaccessible_children().
    If form_id is provided and the stream endpoint contains a '{}' placeholder,
    the formatted endpoint is probed; otherwise the base path before the placeholder
    (or the endpoint itself) is probed.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_cls in STREAMS.items()
        if stream_name in schemas
        and not stream_cls(client=client).check_access(form_id=form_id)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    inaccessible_streams.extend(_prune_inaccessible_children(schemas, field_metadata))

    if not schemas:
        raise TypeformForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have "
            "'read' access to any supported streams."
        )
    elif inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(set(inaccessible_streams)),
        )


def discover(client, form_id=None) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    Access to each stream is verified using the provided client and streams
    the credentials cannot read are excluded from the returned catalog.
    If form_id is provided, stream-specific endpoints are probed for access;
    otherwise the generic forms endpoint is used as a fallback.
    """
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata, form_id=form_id)

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
