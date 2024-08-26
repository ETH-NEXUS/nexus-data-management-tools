#!/usr/bin/env python3

from lk import api
import click
from helpers import TableOutput as T, Message as M
from sys import argv
from config import options_from_source


@click.command()
@click.option("-s", "--schema", envvar="LK_SCHEMA", required=True)
@click.option("-q", "--query", envvar="LK_QUERY", required=True)
@click.option("-c", "--columns", envvar="LK_COLUMNS", required=False, multiple=True)
def get(schema: str, query: str, columns: tuple = ()):
    results = api.query.select_rows(schema_name=schema, query_name=query)
    T.out(results["rows"], headers=columns)
    available_columns = [r for r in results["rows"][0].keys()]
    M.debug(f"Available columns: {available_columns}")


@click.command()
@click.option("-d", "--drop-folder", required=True, type=click.Path())
@click.option("-r", "--repository-folder", default=None, type=click.Path())
@click.option("-p", "--processed-folder", default=None, type=click.Path())
@options_from_source("--drop-folder")
def check(drop_folder: str, repository_folder: str, processed_folder: str):
    print("---")
    print(drop_folder)
    print(repository_folder)
    print(processed_folder)


if __name__ == "__main__":
    if len(argv) < 2:
        M.warn("Please specify a command:")
        M.info(
            """
            get -s schema -q query -c columns
            """
        )
        exit(1)

    if argv[1] in globals().keys():
        globals()[argv[1]](argv[2:])
