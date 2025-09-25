import yaml
import click
import functools
from os.path import isfile, dirname, realpath, join


SCRIPT_DIR = dirname(realpath(__file__))


def configuration_callback(
    option_name,
    ctx,
    parameter,
    value,
):
    """
    Load configuration only from the selected drop folder's sync.yml.
    No global config merging is performed.
    """
    if option_name not in ctx.params:
        raise Exception(f"Option {option_name} not found in {ctx.params.keys()}.")
    sync_config_file = join(ctx.params[option_name], "sync.yml")
    if isfile(sync_config_file):
        with open(sync_config_file, "r") as cf:
            final_config = yaml.safe_load(cf) or {}
    else:
        raise Exception(f"Cannot find sync config in {sync_config_file}.")
    for param in ctx.params.keys():
        if param != option_name:
            ctx.params[param] = ctx.params[param] or final_config.get(param)
    return value


def options_from_source(*options, **attrs):
    """
    Decorator to read config from a selectable path argument.
    Takes the `sync.yml` from the drop folder and populates options.
    """
    options = options or ("--source",)
    option_name = options[0].replace("--", "").replace("-", "_")

    def decorator(f):
        partial_callback = functools.partial(configuration_callback, option_name)
        attrs["callback"] = partial_callback
        return click.option(*options, **attrs)(f)

    return decorator
