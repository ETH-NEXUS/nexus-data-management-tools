#!/usr/bin/env python3

import click
import glob
import re
import shutil
import yaml
import datetime
import json
from os.path import join, isfile, exists, dirname, basename, getmtime
from os import makedirs
from sys import exit

# Package-safe imports with fallback for script execution
try:
    from .helpers import TableOutput as T, Message as M, Hasher
    from .integrity import (
        read_md5_sidecar,
        write_blake3_sidecar,
        copy_matching_sidecar,
    )
    from .metadata import load_metadata_sources
except ImportError:
    from helpers import TableOutput as T, Message as M, Hasher  # type: ignore
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
@click.option(
    "--do-it",
    is_flag=True,
    show_default=True,
    default=False,
    help="if given sync is really done",
)
def sync(
    drop_folder: str,
    do_it: bool,
):
    # Load configuration exclusively from drop_folder/sync.yml
    try:
        with open(join(drop_folder, "sync.yml"), "r") as cf:
            cfg = yaml.safe_load(cf) or {}
    except Exception as ex:
        M.error(f"Failed to load configuration from {join(drop_folder, 'sync.yml')}: {ex}")
        return

    drop_filename_filter = cfg.get("drop_filename_filter")
    drop_filename_regex = cfg.get("drop_filename_regex")
    repository_folder = cfg.get("repository_folder")
    repository_filename = cfg.get("repository_filename")
    processed_folder = cfg.get("processed_folder")
    filename_sequence = cfg.get("filename_sequence")
    date_format = cfg.get("date_format", "%Y-%m-%d %H:%M:%S")
    fields = cfg.get("fields")
    field_parameters = cfg.get("field_parameters", {})
    labkey = cfg.get("labkey", {})
    lookups = cfg.get("lookups")

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

    use_ssl = bool(labkey.get("use_ssl", True))
    context_path = labkey.get("context") or None
    try:
        api = APIWrapper(labkey["host"], labkey["container"], context_path=context_path, use_ssl=use_ssl)
    except (ServerContextError, ServerNotFoundError, QueryNotFoundError) as ex:
        M.error("Labkey connection error:")
        M.error(ex)
        return
    # Quick connection/table checks
    def _lk_check(_schema: str, _table: str, label: str):
        try:
            api.query.select_rows(_schema, _table)
            M.info(f"LabKey check OK for {label}: {_schema}.{_table}")
            return True
        except Exception as ex:
            M.error(f"LabKey check FAILED for {label}: {_schema}.{_table}")
            M.error(ex)
            return False

    # Ensure write-back target exists
    if not _lk_check(labkey.get("schema", ""), labkey.get("table", ""), "write-back table"):
        return

    # Resolve special roles
    file_list_field = list(field_parameters.keys())[list(field_parameters.values()).index("file_list")]
    M.debug(f"file_list_field: {file_list_field}")

    # --- Helpers for pipeline stages ---
    def plan_files() -> list:
        M.debug(f"Using drop_filename_filter: {drop_filename_filter}")
        drop_files = glob.glob(join(drop_folder, drop_filename_filter), recursive=True)
        planned: list = []
        for source_filename in drop_files:
            rel = re.sub(r"^/", "", source_filename.replace(drop_folder, ""))
            match = re.match(drop_filename_regex, rel)
            if not match:
                M.error(f"File {rel} does not match drop_filename_regex!")
                exit(1)
            caps = match.groupdict()
            # Apply before_match replacements to captured variables
            _repl_cfg = (cfg.get("replacements") or {})
            before_match_repls = _repl_cfg.get("before_match") or []
            for rrule in before_match_repls:
                if isinstance(rrule, dict) and rrule.get("target") == "var":
                    name, old, new = rrule.get("name"), rrule.get("replace"), rrule.get("with", "")
                    if name and (name in caps) and isinstance(caps.get(name), str) and (old is not None):
                        try:
                            caps[name] = str(caps[name]).replace(str(old), str(new))
                        except Exception:
                            pass
            # Build initial repository filename
            inter = repository_filename
            for part, value in caps.items():
                inter = re.sub(f"<{part}>", str(value), inter)
            # Sequence handling
            final = inter
            if filename_sequence == "run":
                run = 1
                while True:
                    candidate = re.sub("<run>", str(run), inter)
                    if join(repository_folder, candidate) not in [i["target"] for i in planned]:
                        final = candidate
                        break
                    run += 1
            elif filename_sequence == "hash":
                crc = Hasher.crc32(join(drop_folder, rel))
                final = re.sub("<hash>", crc, inter)
            planned.append({"source": join(drop_folder, rel), "target": join(repository_folder, final), "vars": caps})
        return planned

    def load_and_match_metadata(sync_file_list: list):
        metadata_sources_cfg = cfg.get("metadata_sources")
        metadata_match_cfg = cfg.get("metadata_match")
        if not metadata_sources_cfg:
            return None, None
        M.info("Loading metadata sources...")
        for src in (metadata_sources_cfg or []):
            if (src or {}).get("type") == "labkey":
                _lk_check(src.get("schema", ""), src.get("table", ""), f"metadata source '{src.get('name','')}'")
        sources_result = load_metadata_sources(metadata_sources_cfg, drop_folder, labkey)
        summary = [{"name": r.get("name"), "type": r.get("type"), "count": r.get("count"), "status": r.get("status")} for r in sources_result]
        if summary:
            T.out(summary, sort_by="name", column_options={"justify": "left", "vertical": "middle"})
        sources_by_name = {r.get("name"): r for r in sources_result if r.get("status") == "ok"}
        # Perform metadata matching per file if rules are provided
        if metadata_match_cfg and isinstance(metadata_match_cfg, dict):
            default_key_tmpl = metadata_match_cfg.get("key_template")
            rules = metadata_match_cfg.get("search") or []
            field_index_cache: dict[tuple[str, str], dict[str, dict]] = {}
            for sf in sync_file_list:
                sf["meta_found"], sf["meta_source"], sf["meta_key"] = False, "", ""
                vars_map = sf.get("vars", {})
                for rule in rules:
                    src_name, field = rule.get("source"), rule.get("field")
                    key_tmpl = rule.get("key_template", default_key_tmpl)
                    if not (src_name and field and key_tmpl):
                        continue
                    key_val = key_tmpl
                    for part, value in vars_map.items():
                        key_val = re.sub(f"<{part}>", str(value), key_val)
                    sf["meta_key"] = key_val
                    src = sources_by_name.get(src_name)
                    if not src:
                        M.warn(f"Metadata source '{src_name}' not available; skipping rule")
                        continue
                    # Build index
                    index_key = (src_name, field)
                    if index_key not in field_index_cache:
                        idx = {}
                        for row in src.get("rows", []):
                            v = row.get(field)
                            if v is not None and str(v) not in idx:
                                idx[str(v)] = row
                        field_index_cache[index_key] = idx
                    row_match = field_index_cache[index_key].get(str(key_val))
                    if row_match is not None:
                        meta_rows = sf.setdefault("meta_rows", {})
                        if src_name not in meta_rows:
                            meta_rows[src_name] = row_match
                        if not sf["meta_found"]:
                            sf["meta_found"], sf["meta_source"], sf["meta_row"] = True, src_name, row_match
        return sources_result, sources_by_name

    def derive_and_finalize_targets(sync_file_list: list):
        metadata_derive_cfg = cfg.get("metadata_derive")
        _repl_cfg = (cfg.get("replacements") or {})
        before_match_repls = _repl_cfg.get("before_match") or []
        if metadata_derive_cfg and isinstance(metadata_derive_cfg, list):
            for sf in sync_file_list:
                for rule in metadata_derive_cfg:
                    src_name, field, pattern = rule.get("source"), rule.get("field"), rule.get("regex")
                    if not (src_name and field and pattern):
                        continue
                    row = (sf.get("meta_rows") or {}).get(src_name)
                    if row is None and sf.get("meta_source") == src_name:
                        row = sf.get("meta_row")
                    if not row:
                        continue
                    value = row.get(field)
                    if value is None:
                        continue
                    try:
                        m = re.search(pattern, str(value))
                    except re.error as ex:
                        M.warn(f"Invalid regex in metadata_derive for {src_name}.{field}: {ex}")
                        continue
                    if m:
                        groups = m.groupdict()
                        if groups:
                            existing = sf.get("vars") or {}
                            sf["vars"] = existing
                            for k, v in groups.items():
                                if v is not None and k not in existing:
                                    existing[k] = v
            # Apply before_match replacements to derived vars
            for sf in sync_file_list:
                vars_map = sf.get("vars") or {}
                for rrule in before_match_repls:
                    if isinstance(rrule, dict) and rrule.get("target") == "var":
                        name, old, new = rrule.get("name"), rrule.get("replace"), rrule.get("with", "")
                        if name and (name in vars_map) and isinstance(vars_map.get(name), str) and (old is not None):
                            try:
                                vars_map[name] = str(vars_map[name]).replace(str(old), str(new))
                            except Exception:
                                pass
        # Recompute target paths after metadata
        resolved_targets: set[str] = set()
        meta_placeholder_re = re.compile(r"<([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)>")
        for sf in sync_file_list:
            tmpl = repository_filename
            for part, value in (sf.get("vars") or {}).items():
                tmpl = re.sub(f"<{part}>", str(value), tmpl)
            def _meta_sub(m):
                src_name, field_name = m.group(1), m.group(2)
                row = (sf.get("meta_rows") or {}).get(src_name)
                if row is None and sf.get("meta_source") == src_name:
                    row = sf.get("meta_row")
                if row is not None and (field_name in row) and (row[field_name] is not None):
                    return str(row[field_name])
                return ""
            tmpl = meta_placeholder_re.sub(_meta_sub, tmpl)
            if filename_sequence == "run":
                run = 1
                while True:
                    candidate = re.sub("<run>", str(run), tmpl)
                    target_full = join(repository_folder, candidate)
                    if target_full not in resolved_targets:
                        resolved_targets.add(target_full)
                        sf["target"] = target_full
                        break
                    run += 1
            elif filename_sequence == "hash":
                crc = Hasher.crc32(sf["source"])
                candidate = re.sub("<hash>", crc, tmpl)
                sf["target"] = join(repository_folder, candidate)
            else:
                sf["target"] = join(repository_folder, tmpl)
        # Preserve variables and metadata for writeback
        for sf in sync_file_list:
            if "tmpl_vars" not in sf:
                sf["tmpl_vars"] = dict(sf.get("vars") or {})
            if "meta_for_write" not in sf:
                sf["meta_for_write"] = {
                    "primary_source": sf.get("meta_source"),
                    "primary_row": sf.get("meta_row"),
                    "rows_by_source": dict((sf.get("meta_rows") or {})),
                }
            sf.pop("vars", None)
            sf.pop("meta_row", None)
            sf.pop("meta_rows", None)
        return sync_file_list

    def check_integrity(sync_file_list: list):
        M.info("Checking source integrity using md5 sidecars (if present)...")
        for sf in sync_file_list:
            expected_md5 = read_md5_sidecar(sf["source"])
            if expected_md5:
                md5_of_file = Hasher.md5(sf["source"])
                sf["integrity_method"], sf["md5"], sf["orig_md5"], sf["md5_ok"] = "md5", md5_of_file, expected_md5, (md5_of_file == expected_md5)
            else:
                sf["integrity_method"], sf["md5"], sf["orig_md5"], sf["md5_ok"] = "blake3", None, None, None
        return sync_file_list

    def check_labkey_presence(sync_file_list: list):
        M.info("Collecting information from labkey...")
        for sf in sync_file_list:
            try:
                filters = [QueryFilter(file_list_field, sf["target"], QueryFilter.Types.CONTAINS)]
                results = api.query.select_rows(labkey["schema"], labkey["table"], filter_array=filters)
                sf["in_labkey"] = len(results["rows"]) > 0
            except RequestError as ex:
                M.error("Labkey request error:")
                M.error(ex)
        return sync_file_list

    def render_plan_table(rows: list):
        T.out(
            rows,
            sort_by="source",
            column_options={"justify": "left", "vertical": "middle"},
            row_style=lambda row: (
                "red"
                if row.get("integrity_method") == "md5" and (row.get("md5_ok") is False)
                else ("yellow" if ("meta_found" in row and row.get("meta_found") is False) else None)
            ),
        )

    def perform_copy_and_writeback(sync_file_list: list):
        synced_file_list = []
        writeback_rows = []
        for sync_file in sync_file_list:
            # Copy source to target
            copy_ok = False
            # Policy: require metadata match before copying
            if metadata_required and not sync_file.get("meta_found", False):
                M.warn(f"Skipping {sync_file['source']} due to missing metadata (metadata_required).")
                synced_file_list.append({
                    "source": sync_file["source"],
                    "target": sync_file["target"],
                    "copy_ok": False,
                    "verified": False,
                    "reason": "metadata_missing",
                    "sidecar": "",
                    "sidecar_copy_ok": False,
                })
                continue
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
            synced_file_list.append({
                "source": sync_file["source"],
                "target": sync_file["target"],
                "copy_ok": copy_ok,
                "verified": verified,
                "reason": "",
                "sidecar": sidecar,
                "sidecar_copy_ok": sidecar_copy_ok,
            })
            # Prepare LabKey row for write-back when verified
            if verified and fields and isinstance(fields, dict):
                # Load replacements config for write-back
                _repl_cfg = (cfg.get("replacements") or {})
                before_wb_repls = _repl_cfg.get("before_writeback") or []
                writeback_rows.append(_build_row(sync_file, fields, before_wb_repls))

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

        if writeback_rows:
            try:
                api.query.insert_rows(labkey["schema"], labkey["table"], writeback_rows)
                M.info(f"Inserted {len(writeback_rows)} row(s) into LabKey {labkey['schema']}.{labkey['table']}")
            except Exception as ex:
                M.error("Labkey insert error:")
                M.error(ex)
        return

    def dry_run_writeback(sync_file_list: list):
        if not (fields and isinstance(fields, dict)):
            return
        writeback_rows = []
        _repl_cfg = (cfg.get("replacements") or {})
        before_wb_repls = _repl_cfg.get("before_writeback") or []
        for sf in sync_file_list:
            writeback_rows.append(_build_row(sf, fields, before_wb_repls))
        try:
            M.info("Planned LabKey rows (dry run):")
            print(json.dumps(writeback_rows, indent=2))
        except Exception:
            pass
        return

    # --- Helpers to render fields and build LabKey rows ---
    def _render_value(sync_file: dict, vars_map: dict, val: str) -> object:
        if not isinstance(val, str):
            return val
        out = val
        # Replace <var> placeholders
        for part, value in vars_map.items():
            out = re.sub(f"<{part}>", str(value), out)
        # Replace <source.Field> placeholders
        def _meta_sub(m):
            src_name, field_name = m.group(1), m.group(2)
            meta = sync_file.get("meta_for_write") or {}
            row = (meta.get("rows_by_source") or {}).get(src_name)
            if row is None and meta.get("primary_source") == src_name:
                row = meta.get("primary_row")
            if row is not None and (field_name in row) and (row[field_name] is not None):
                return str(row[field_name])
            return ""
        out = re.sub(r"<([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)>", _meta_sub, out)
        # Functions
        out = out.replace("now()", datetime.datetime.now().strftime(date_format))
        if "drop_file_mtime()" in out:
            try:
                mtime = datetime.datetime.fromtimestamp(getmtime(sync_file["source"]))
                out = out.replace("drop_file_mtime()", mtime.strftime(date_format))
            except Exception:
                out = out.replace("drop_file_mtime()", "")
        return out

    def _build_row(sync_file: dict, fields_cfg: dict, before_wb_repls: list) -> dict:
        vars_map = dict(sync_file.get("tmpl_vars") or {})
        # Apply var-target replacements before rendering
        for rrule in before_wb_repls or []:
            if not isinstance(rrule, dict):
                continue
            if rrule.get("target") == "var":
                name = rrule.get("name")
                old = rrule.get("replace")
                new = rrule.get("with", "")
                if name and (name in vars_map) and isinstance(vars_map.get(name), str) and (old is not None):
                    try:
                        vars_map[name] = str(vars_map[name]).replace(str(old), str(new))
                    except Exception:
                        pass
        # Convenience variables
        vars_map.setdefault("uploaded_filename", basename(sync_file["source"]))
        vars_map.setdefault("target_path", sync_file["target"])
        # Render row
        row: dict = {}
        for k, v in (fields_cfg or {}).items():
            row[k] = _render_value(sync_file, vars_map, v)
        # Auto-fill common fields if present
        if "Path_To_Synced_Data" in fields_cfg:
            row["Path_To_Synced_Data"] = sync_file["target"]
        if "Uploaded_File_Name" in fields_cfg:
            row["Uploaded_File_Name"] = basename(sync_file["source"])
        if "Md5sum" in fields_cfg:
            try:
                # Only compute if target exists (dry-run may not copy)
                if isfile(sync_file["target"]):
                    row["Md5sum"] = Hasher.md5(sync_file["target"])
            except Exception:
                pass
        # Apply field-target replacements after rendering
        for rrule in before_wb_repls or []:
            if not isinstance(rrule, dict):
                continue
            if rrule.get("target") == "field":
                name = rrule.get("name")
                old = rrule.get("replace")
                new = rrule.get("with", "")
                if name and (name in row) and isinstance(row.get(name), str) and (old is not None):
                    try:
                        row[name] = str(row[name]).replace(str(old), str(new))
                    except Exception:
                        pass
        return row

    # --- Run pipeline ---
    sync_file_list = plan_files()
    sources_result, sources_by_name = load_and_match_metadata(sync_file_list)
    metadata_required = bool(cfg.get("metadata_required", False))
    if metadata_required and not cfg.get("metadata_sources"):
        M.warn("metadata_required is True but no metadata_sources configured; all files will be skipped.")
    if metadata_required and not cfg.get("metadata_match"):
        M.warn("metadata_required is True but no metadata_match rules defined; all files will be skipped.")
    sync_file_list = derive_and_finalize_targets(sync_file_list)
    sync_file_list = check_integrity(sync_file_list)
    sync_file_list = check_labkey_presence(sync_file_list)
    render_plan_table(sync_file_list)

    if do_it:
        _continue = input("Do you want to sync the files in the list? (y/n): ")
        if _continue.lower() in ["yes", "y"]:
            perform_copy_and_writeback(sync_file_list)
    else:
        dry_run_writeback(sync_file_list)
if __name__ == "__main__":
    cli()
