from datetime import datetime as dt, timezone as tz
from os.path import getmtime, isfile


def now(date_format: str):
    return dt.strftime(dt.now(tz.utc).astimezone(), date_format)


def drop_file_mtime(filename, date_format):
    if not isfile(filename):
        raise Exception(
            f"Cannot get mtime of file {filename}, because it does not exist."
        )
    return dt.strftime(dt.fromtimestamp(getmtime(filename)), date_format)
