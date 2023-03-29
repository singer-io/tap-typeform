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

def validate_form_ids(client, config):
    """Validate the form ids passed in the config"""
    form_stream = Forms()

    config_forms = _forms_to_list(config)
    api_forms = {form.get('id') for res in form_stream.get_forms(client) for form in res if form != ''}

    if config_forms is None:
        LOGGER.info("No form ids provided in config; fetching all forms")
        return api_forms
    
    mismatched_forms = config_forms.difference(api_forms)
    if len(mismatched_forms) > 0:
        # Raise an error if any form-id from config is not matching
        # from ids from API response
        raise FormMistmatchError("FormMistmatchError: unable to find forms {}".format(mismatched_forms))
    
    return config_forms

    


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    client = Client(config)

    forms_to_sync = validate_form_ids(client, config)

    if args.discover:
        catalog = _discover()
        catalog.dump()
    else:
        catalog = args.catalog \
            if args.catalog else _discover()
        _sync(client, config, args.state, catalog.to_dict(), forms_to_sync)

if __name__ == "__main__":
    main()
