#!/usr/bin/env python3
import singer
from singer import utils
from tap_typeform.discover import discover as _discover
from tap_typeform.sync import _forms_to_list, sync as _sync
from tap_typeform.client import Client
from tap_typeform.streams import Forms

REQUIRED_CONFIG_KEYS = ["start_date", "token", "forms"]

LOGGER = singer.get_logger()

class FormMistmatchError(Exception):
    pass

class NoFormsProvidedError(Exception):
    pass

def validate_form_ids(config):
    """Validate the form ids passed in the config"""
    client = Client(config)
    form_stream = Forms()

    if not config.get('forms'):
        LOGGER.fatal("No forms were provided in config")
        raise NoFormsProvidedError

    config_forms = _forms_to_list(config)
    api_forms = {form.get('id') for res in form_stream.get_forms(client) for form in res}

    mismatched_forms = config_forms.difference(api_forms)

    if len(mismatched_forms) > 0:
        LOGGER.fatal(f"FormMistmatchError: forms {mismatched_forms} not returned by API")
        raise FormMistmatchError


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    client = Client(config)
    validate_form_ids(config)
    if args.discover:
        catalog = _discover()
        catalog.dump()
    else:
        catalog = args.catalog \
            if args.catalog else _discover()
        _sync(client, config, args.state, catalog.to_dict())

if __name__ == "__main__":
    main()
