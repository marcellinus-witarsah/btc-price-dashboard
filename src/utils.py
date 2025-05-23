from configparser import ConfigParser
import pytz
from datetime import datetime


def load_config(filename, section):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(
            "Section {0} not found in the {1} file".format(section, filename)
        )

    return config


def convert_unixtime_to_timestamp_tz(unixtime, timezone_str):
    datetime_obj = datetime.fromtimestamp(unixtime)
    tz = pytz.timezone(timezone_str)
    return tz.localize(datetime_obj)
