import yaml
import click
import functools
from os.path import isfile, dirname, realpath, join
from deepmerge import always_merger


SCRIPT_DIR = dirname(realpath(__file__))


def configuration_callback(
    option_name,
    ctx,
    parameter,
    value,
):
    """
    Let's chain the configuration like local config, so config file
    in the source directory has precedence over the global config.
    """
    global_config_file = join(SCRIPT_DIR, "sync.yml")
    global_config = {}
    if isfile(global_config_file):
        with open(global_config_file, "r") as cf:
            global_config = yaml.safe_load(cf)
    sync_config = {}
    if option_name not in ctx.params:
        raise Exception(f"Option {option_name} not found in {ctx.params.keys()}.")
    sync_config_file = join(ctx.params[option_name], "sync.yml")
    if isfile(sync_config_file):
        with open(sync_config_file, "r") as cf:
            sync_config = yaml.safe_load(cf)
    else:
        raise Exception(f"Cannot find sync config in {sync_config_file}.")
    # Override the global config with the local values
    # final_config = global_config | sync_config
    final_config = always_merger.merge(global_config, sync_config)
    for param in ctx.params.keys():
        if param != option_name:
            # If the option is given as a command line argument
            # it will take the given value else it takes the configured
            # value.
            ctx.params[param] = ctx.params[param] or (
                final_config[param] if param in final_config else None
            )
    return value


def options_from_source(*options, **attrs):
    """
    Decorator to read the config from a selectable path argument.
    This will take the `sync.yml` from the source folder to
    overwrite global configuration.
    """
    options = options or ("--source",)
    option_name = options[0].replace("--", "").replace("-", "_")

    def decorator(f):
        partial_callback = functools.partial(configuration_callback, option_name)
        attrs["callback"] = partial_callback
        return click.option(*options, **attrs)(f)

    return decorator
