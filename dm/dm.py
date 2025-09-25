#!/usr/bin/env python3

import click
import glob
import re
import functions
import shutil
from copy import deepcopy
from os.path import join, isfile, exists, dirname
from os import makedirs
from helpers import TableOutput as T, Message as M, Hasher
from sys import argv, exit
from config import options_from_source

from labkey.api_wrapper import APIWrapper
from labkey.query import QueryFilter
from labkey.exceptions import (
    RequestError,
    QueryNotFoundError,
    ServerContextError,
    ServerNotFoundError,
)


@click.command()
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
    if not repository_folder:
        M.error("Please define 'repository_folder'.")
    if not repository_filename:
        M.error("Please define 'repository_filename'.")
    if not processed_folder:
        M.error("Please define 'processed_folder'.")

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
    file_list_aggregator_field = list(field_parameters.keys())[
        list(field_parameters.values()).index("file_list_aggregator")
    ]
    M.debug(
        f"file_list_field: {file_list_field}, file_list_aggregator_field: {file_list_aggregator_field}"
    )

    M.debug(f"Using drop_filename_filter: {drop_filename_filter}")
    drop_files = glob.glob(join(drop_folder, drop_filename_filter), recursive=True)
    sync_file_list = []
    rows = []
    for source_filename in drop_files:
        # source_filename should not contain the drop_folder part
        source_filename = re.sub(r"^/", "", source_filename.replace(drop_folder, ""))
        # M.debug("---")
        # M.debug(f"Found file: {source_filename}")
        match = re.match(drop_filename_regex, source_filename)
        if not match:
            M.error(f"File {source_filename} does not match drop_filename_regex!")
            exit(1)
        replacements = match.groupdict()
        # M.debug(replacements)
        intermediate_repository_filename = repository_filename
        for part, value in replacements.items():
            intermediate_repository_filename = re.sub(
                f"<{part}>", str(value), intermediate_repository_filename
            )

        # Check if the destination file already exists and then count up the <run>
        # special handling if filename_sequence is run
        if filename_sequence == "run":
            run = 1
            while True:
                final_repository_filename = re.sub(
                    "<run>", str(run), intermediate_repository_filename
                )
                # If the target file already exist in the list we increment the run
                # by 1 and replace the <run> in the target filename.
                if join(repository_folder, final_repository_filename) not in [
                    item["target"] for item in sync_file_list
                ]:
                    break
                run += 1
            replacements["run"] = str(run)
        elif filename_sequence == "hash":
            crc = Hasher.crc32(join(drop_folder, source_filename))
            final_repository_filename = re.sub(
                "<hash>", crc, intermediate_repository_filename
            )
            replacements["hash"] = crc

        # M.info(
        #     f"{join(drop_folder, source_filename)} ->\n{join(repository_folder, final_repository_filename)}\n"
        # )
        sync_file_list.append(
            {
                "source": join(drop_folder, source_filename),
                "target": join(repository_folder, final_repository_filename),
            }
        )

        row = {}
        for field, value in fields.items():

            if isinstance(value, str) and value.endswith("()"):
                if value == "now()":
                    final_value = functions.now(date_format)
                elif value == "drop_file_mtime()":
                    final_value = functions.drop_file_mtime(
                        join(drop_folder, source_filename), date_format
                    )
                else:
                    final_value = f"Unknown function: {value}"
            elif field == file_list_field:
                final_value = join(repository_folder, final_repository_filename)
            else:
                final_value = str(value)
                for part, _value in replacements.items():
                    if part in lookups:
                        _value = lookups[part][_value]
                    final_value = re.sub(f"<{part}>", str(_value), final_value)
            row[field] = final_value
        # M.debug(row)
        rows.append(row)

        # Handle the file list aggregation
        aggregated_rows = {}
        for row in rows:
            if row[file_list_aggregator_field] not in aggregated_rows:
                aggregated_rows[row[file_list_aggregator_field]] = deepcopy(row)
            else:
                aggregated_rows[row[file_list_aggregator_field]][
                    file_list_field
                ] += f", {row[file_list_field]}"
            # M.debug(
            #     f"{row[file_list_aggregator_field]}: {aggregated_rows[row[file_list_aggregator_field]]}"
            # )
            # return
        aggregated_rows = [row for row in aggregated_rows.values()]
    # T.out(aggregated_rows, headers=fields.keys())

    ###
    # Pre-copy integrity check plan
    # - If an .md5 sidecar exists: compute MD5 and compare to sidecar value
    # - If no .md5 sidecar: we will compute a .blake3 sidecar during copy step
    ###
    M.info("Checking source integrity using md5 sidecars (if present)...")
    for sync_file in sync_file_list:
        md5_filename = f"{sync_file['source']}.md5"
        if isfile(md5_filename):
            orig_md5 = None
            with open(md5_filename, "r") as f:
                first_line = f.readline().split(None, 1)
                if len(first_line) > 0:
                    orig_md5 = first_line[0]
            md5_of_file = Hasher.md5(sync_file["source"]) if orig_md5 else None
            sync_file["integrity_method"] = "md5"
            sync_file["md5"] = md5_of_file
            sync_file["orig_md5"] = orig_md5
            sync_file["md5_ok"] = (md5_of_file == orig_md5) if (md5_of_file and orig_md5) else False
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
                    "Path_To_Synced_Data",
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
                ###
                # Copy source to target
                ###
                copy_ok = False
                # Pre-copy integrity enforcement
                md5_filename = f"{sync_file['source']}.md5"
                if isfile(md5_filename):
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
                    try:
                        b3 = Hasher.blake3(sync_file["source"])  # streamed
                        blake3_filename = f"{sync_file['source']}.blake3"
                        with open(blake3_filename, "w") as bf:
                            bf.write(f"{b3}\n")
                    except Exception as ex:
                        M.warn(f"Could not write blake3 sidecar for {sync_file['source']}: {ex}")
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
                    src_md5 = f"{sync_file['source']}.md5"
                    src_b3 = f"{sync_file['source']}.blake3"
                    if isfile(src_md5):
                        sidecar = "md5"
                        dst_md5 = f"{sync_file['target']}.md5"
                        try:
                            shutil.copyfile(src_md5, dst_md5)
                            sidecar_copy_ok = isfile(dst_md5)
                        except Exception as ex:
                            M.warn(f"Could not copy md5 sidecar for {sync_file['source']}: {ex}")
                    elif isfile(src_b3):
                        sidecar = "blake3"
                        dst_b3 = f"{sync_file['target']}.blake3"
                        try:
                            shutil.copyfile(src_b3, dst_b3)
                            sidecar_copy_ok = isfile(dst_b3)
                        except Exception as ex:
                            M.warn(f"Could not copy blake3 sidecar for {sync_file['source']}: {ex}")
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

                ###
                # Post-copy verification done via block-by-block compare above
                ###

                ###
                # Add rows to labkey if not already exist
                ###

                ###
                # If all good check again and move the drop to processed
                ###

                ###
                #
                ###
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


@click.command()
@click.option("-h", "--host", required=True, type=click.Path())
@click.option("-c", "--container", required=True, type=click.Path())
@click.option("-s", "--schema", required=True, type=click.Path())
@click.option("-t", "--table", required=True, type=click.Path())
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
    if len(argv) < 2:
        M.warn("Please specify a command:")
        M.info(
            """
            sync -d path/to/data-drop
            check -h host -c container -s schema -t table
            """
        )
        exit(1)

    if argv[1] in globals().keys():
        globals()[argv[1]](argv[2:])
