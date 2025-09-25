#!/usr/bin/env python3

import click
import glob
import re
import shutil
import yaml
from os.path import join, isfile, exists, dirname
from os import makedirs
from sys import exit

# Package-safe imports with fallback for script execution
try:
    from .helpers import TableOutput as T, Message as M, Hasher
    from .config import options_from_source
    from .integrity import (
        read_md5_sidecar,
        write_blake3_sidecar,
        copy_matching_sidecar,
    )
    from .metadata import load_metadata_sources
except ImportError:
    from helpers import TableOutput as T, Message as M, Hasher  # type: ignore
    from config import options_from_source  # type: ignore
    from integrity import (  # type: ignore
        read_md5_sidecar,
        write_blake3_sidecar,
        copy_matching_sidecar,
    )
    from metadata import load_metadata_sources  # type: ignore

from labkey.api_wrapper import APIWrapper
from labkey.query import QueryFilter
from labkey.exceptions import (
    RequestError,
    QueryNotFoundError,
    ServerContextError,
    ServerNotFoundError,
)


@click.group()
def cli():
    """Nexus Data Management CLI"""
    pass


@cli.command()
@click.option("-d", "--drop-folder", required=True, type=click.Path())
@click.option("-f", "--drop-filename-filter", default=None, type=str)
@click.option("-x", "--drop-filename-regex", default=None, type=str)
@click.option("-r", "--repository-folder", default=None, type=click.Path())
@click.option("-t", "--repository-filename", default=None, type=str)
@click.option("-p", "--processed-folder", default=None, type=click.Path())
@click.option("--filename-sequence", default=None, type=click.Choice(["run", "hash"]))
@click.option("--date-format", default=None, type=str)
@click.option(
    "--do-it",
    is_flag=True,
    show_default=True,
    default=False,
    help="if given sync is really done",
)
@click.option("--fields", default=None)
@click.option("--field-parameters", default=None)
@click.option("--labkey", default=None)
@click.option("--lookups", default=None)
@options_from_source("--drop-folder")
def sync(
    drop_folder: str,
    drop_filename_filter: str,
    drop_filename_regex: str,
    repository_folder: str,
    repository_filename: str,
    processed_folder: str,
    do_it: bool,
    filename_sequence: str,
    date_format: str,
    fields: dict,
    field_parameters: dict,
    labkey: dict,
    lookups: dict,
):
    if not drop_filename_filter:
        M.error("Please define 'drop_filename_filter'.")
        return
    if not drop_filename_regex:
        M.error("Please define 'drop_filename_regex'.")
        return
    if not repository_folder:
        M.error("Please define 'repository_folder'.")
        return
    if not repository_filename:
        M.error("Please define 'repository_filename'.")
        return
    if not processed_folder:
        M.error("Please define 'processed_folder'.")
        return

    try:
        api = APIWrapper(labkey["host"], labkey["container"], use_ssl=True)
    except ServerContextError as ex:
        M.error("Labkey server context error:")
        M.error(ex)
        return
    except ServerNotFoundError as ex:
        M.error("Labkey server not found error:")
        M.error(ex)
        return
    except QueryNotFoundError as ex:
        M.error("Labkey query not found error:")
        M.error(ex)
        return

    file_list_field = list(field_parameters.keys())[
        list(field_parameters.values()).index("file_list")
    ]
    M.debug(f"file_list_field: {file_list_field}")

    M.debug(f"Using drop_filename_filter: {drop_filename_filter}")
    drop_files = glob.glob(join(drop_folder, drop_filename_filter), recursive=True)
    sync_file_list = []
    for source_filename in drop_files:
        # Remove drop_folder prefix from source path for regex matching
        source_filename = re.sub(r"^/", "", source_filename.replace(drop_folder, ""))
        match = re.match(drop_filename_regex, source_filename)
        if not match:
            M.error(f"File {source_filename} does not match drop_filename_regex!")
            exit(1)
        replacements = match.groupdict()
        intermediate_repository_filename = repository_filename
        for part, value in replacements.items():
            intermediate_repository_filename = re.sub(
                f"<{part}>", str(value), intermediate_repository_filename
            )

        # Compute final target filename with collision handling or hash, and
        # ensure it is always defined even if filename_sequence is unset.
        final_repository_filename = intermediate_repository_filename
        if filename_sequence == "run":
            run = 1
            while True:
                candidate = re.sub("<run>", str(run), intermediate_repository_filename)
                if join(repository_folder, candidate) not in [
                    item["target"] for item in sync_file_list
                ]:
                    final_repository_filename = candidate
                    break
                run += 1
        elif filename_sequence == "hash":
            crc = Hasher.crc32(join(drop_folder, source_filename))
            final_repository_filename = re.sub("<hash>", crc, intermediate_repository_filename)

        sync_file_list.append(
            {
                "source": join(drop_folder, source_filename),
                "target": join(repository_folder, final_repository_filename),
            }
        )

    # Load external metadata sources from the local drop folder YAML and print a summary
    metadata_sources_cfg = None
    try:
        with open(join(drop_folder, "sync.yml"), "r") as cf:
            _local_cfg = yaml.safe_load(cf) or {}
            metadata_sources_cfg = _local_cfg.get("metadata_sources")
    except Exception:
        metadata_sources_cfg = None
    if metadata_sources_cfg:
        M.info("Loading metadata sources...")
        sources_result = load_metadata_sources(metadata_sources_cfg, drop_folder)
        summary = [
            {"name": r.get("name"), "type": r.get("type"), "count": r.get("count"), "status": r.get("status")}
            for r in sources_result
        ]
        if summary:
            T.out(summary, sort_by="name", column_options={"justify": "left", "vertical": "middle"})

    # Building LabKey rows is deferred until write-back is implemented.

    # Pre-copy integrity plan:
    # - If a .md5 sidecar exists: compute MD5 and compare to sidecar value
    # - If no .md5 sidecar: compute a .blake3 sidecar before copy
    M.info("Checking source integrity using md5 sidecars (if present)...")
    for sync_file in sync_file_list:
        expected_md5 = read_md5_sidecar(sync_file["source"])
        if expected_md5:
            md5_of_file = Hasher.md5(sync_file["source"])
            sync_file["integrity_method"] = "md5"
            sync_file["md5"] = md5_of_file
            sync_file["orig_md5"] = expected_md5
            sync_file["md5_ok"] = md5_of_file == expected_md5
        else:
            # No md5 sidecar present; we'll create a .blake3 sidecar before copy
            sync_file["integrity_method"] = "blake3"
            sync_file["md5"] = None
            sync_file["orig_md5"] = None
            sync_file["md5_ok"] = None

    # Comparing with labkey
    M.info("Collecting information from labkey...")
    for sync_file in sync_file_list:
        try:
            filters = [
                QueryFilter(
                    file_list_field,
                    sync_file["target"],
                    QueryFilter.Types.CONTAINS,
                ),
            ]
            results = api.query.select_rows(
                labkey["schema"], labkey["table"], filter_array=filters
            )
            sync_file["in_labkey"] = len(results["rows"]) > 0
        except RequestError as ex:
            M.error("Labkey request error:")
            M.error(ex)

    T.out(
        sync_file_list,
        sort_by="source",
        column_options={"justify": "left", "vertical": "middle"},
        row_style=lambda row: (
            "red" if row.get("integrity_method") == "md5" and (row.get("md5_ok") is False) else None
        ),
    )

    ###
    # If do-it is given: ask if the files in the table should be synced
    ###
    if do_it:
        synced_file_list = []
        _continue = input("Do you want to sync the files in the list? (y/n): ")
        if _continue.lower() in ["yes", "y"]:
            for sync_file in sync_file_list:
                # Copy source to target
                copy_ok = False
                # Pre-copy integrity enforcement
                expected_md5 = read_md5_sidecar(sync_file["source"])
                if expected_md5 is not None:
                    # If md5 sidecar exists, ensure it matches before copying
                    if sync_file.get("md5_ok") is False:
                        M.error(
                            f"MD5 mismatch for {sync_file['source']} (expected {sync_file.get('orig_md5')}, got {sync_file.get('md5')}). Skipping copy."
                        )
                        synced_file_list.append(
                            {
                                "source": sync_file["source"],
                                "target": sync_file["target"],
                                "copy_ok": False,
                                "verified": False,
                                "reason": "md5_mismatch",
                                "sidecar": "md5",
                                "sidecar_copy_ok": False,
                            }
                        )
                        continue
                else:
                    # No md5 sidecar; compute and write a .blake3 sidecar before copy
                    write_blake3_sidecar(sync_file["source"])
                if not exists(sync_file["target"]):
                    makedirs(dirname(sync_file["target"]), exist_ok=True)
                    target = shutil.copyfile(sync_file["source"], sync_file["target"])
                    if isfile(target):
                        copy_ok = True
                # Post-copy verification using block-by-block comparison
                verified = False
                if isfile(sync_file["target"]):
                    verified = Hasher.equals(sync_file["source"], sync_file["target"])
                # Copy sidecar after successful verification
                sidecar = ""
                sidecar_copy_ok = False
                if verified:
                    sidecar, sidecar_copy_ok = copy_matching_sidecar(
                        sync_file["source"], sync_file["target"]
                    )
                synced_file_list.append(
                    {
                        "source": sync_file["source"],
                        "target": sync_file["target"],
                        "copy_ok": copy_ok,
                        "verified": verified,
                        "reason": "",
                        "sidecar": sidecar,
                        "sidecar_copy_ok": sidecar_copy_ok,
                    }
                )

            T.out(
                synced_file_list,
                sort_by="source",
                column_options={"justify": "left", "vertical": "middle"},
                row_style=lambda row: (
                    "yellow"
                    if not row.get("copy_ok")
                    else (
                        "red"
                        if (
                            not row.get("verified")
                            or (
                                row.get("sidecar") in ("md5", "blake3")
                                and not row.get("sidecar_copy_ok")
                            )
                        )
                        else None
                    )
                ),
            )


@cli.command()
@click.option("-h", "--host", required=True, type=click.STRING)
@click.option("-c", "--container", required=True, type=click.STRING)
@click.option("-s", "--schema", required=True, type=click.STRING)
@click.option("-t", "--table", required=True, type=click.STRING)
def check(host: str, container: str, schema: str, table: str):
    try:
        api = APIWrapper(host, container, use_ssl=True)
        result = api.query.select_rows(schema, table)
        print(result)
    except ServerContextError as ex:
        M.error("Labkey server context error:")
        M.error(ex)
        return
    except ServerNotFoundError as ex:
        M.error("Labkey server not found error:")
        M.error(ex)
        return
    except QueryNotFoundError as ex:
        M.error("Labkey query not found error:")
        M.error(ex)
        return


if __name__ == "__main__":
    cli()
