import json


def read_config(config_path):
    """
    Performs read on the provided filepath,
    returns empty dict if invalid path provided
    """
    try:
        with open(config_path, "r") as tap_config:
            return json.load(tap_config)
    except FileNotFoundError as err:
        raise Exception("Failed to load config in dev mode") from err


def write_config(config_path, data):
    """
    Updates the provided filepath with json format of the `data` object
    """
    config = read_config(config_path)
    config.update(data)
    with open(config_path, "w") as tap_config:
        json.dump(config, tap_config, indent=2)
    return config
