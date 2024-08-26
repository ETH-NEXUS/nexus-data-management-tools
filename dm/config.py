import yaml
import click
from os import environ
from os.path import isfile, dirname, realpath, join
from dotenv import load_dotenv
from easydict import EasyDict
import functools


SCRIPT_DIR = dirname(realpath(__file__))

# Load .env
if isfile(".env"):
    load_dotenv(".env")

# Load config
with open(join(SCRIPT_DIR, "dm.conf.yml"), "r") as cf:
    config = EasyDict(yaml.safe_load(cf))[environ.get("DM_ENV", "default")]


def configuration_callback(
    option_name,
    ctx,
    parameter,
    value,
):
    sync_config = {}
    if option_name not in ctx.params:
        raise Exception(f"Option {option_name} not found in {ctx.params.keys()}.")
    sync_config_file = join(ctx.params[option_name], ".sync.yml")
    if isfile(sync_config_file):
        with open(sync_config_file, "r") as cf:
            sync_config = yaml.safe_load(cf)
    for param in ctx.params.keys():
        if param != option_name:
            ctx.params[param] = (
                ctx.params[param] or sync_config[param]
                if param in sync_config
                else None
            )

    return value


def options_from_source(*options, **attrs):
    """
    Decorator to read the config from a selectable path argument.
    """
    options = options or ("--source",)
    option_name = options[0].replace("--", "").replace("-", "_")

    def decorator(f):
        partial_callback = functools.partial(configuration_callback, option_name)
        attrs["callback"] = partial_callback
        return click.option(*options, **attrs)(f)

    return decorator
