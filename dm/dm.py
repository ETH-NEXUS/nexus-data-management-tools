#!/usr/bin/env python3

import click
import glob
import re
import shutil
import yaml
import datetime
import json
import sys
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
    # Initialize logfile tee: logs/sync/<runmode>/<dataset>-<timestamp>.log
    try:
        runmode = "run" if do_it else "dry-run"
        dataset = basename(drop_folder.rstrip("/")) or "drop"
        logs_dir = join(dirname(__file__), "logs", "sync", runmode)
        makedirs(logs_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_path = join(logs_dir, f"{dataset}-{timestamp}.log")

        class _Tee:
            def __init__(self, *streams):
                self._streams = tuple(streams)
            def write(self, data):
                for s in self._streams:
                    try:
                        s.write(data)
                    except Exception:
                        pass
            def flush(self):
                for s in self._streams:
                    try:
                        s.flush()
                    except Exception:
                        pass

        _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
        _log_fp = open(log_path, "w", encoding="utf-8")
        sys.stdout = _Tee(_orig_stdout, _log_fp)
        sys.stderr = _Tee(_orig_stderr, _log_fp)
        M.info(f"Log file: {log_path}")
    except Exception as ex:
        M.warn(f"Unable to initialize logfile: {ex}")
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
        skipped: list = []
        for source_filename in drop_files:
            rel = re.sub(r"^/", "", source_filename.replace(drop_folder, ""))
            match = re.match(drop_filename_regex, rel)
            if not match:
                skipped.append(rel)
                continue
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

        # Log discovery and selection summary
        M.info(f"Discovered {len(drop_files)} file(s) via drop_filename_filter; matched {len(planned)}, skipped {len(skipped)} by drop_filename_regex.")
        if planned:
            try:
                T.out(
                    [
                        {"source": re.sub(r"^/", "", p["source"].replace(drop_folder, "")), "status": "matched"}
                        for p in planned
                    ],
                    sort_by="source",
                    column_options={"justify": "left", "vertical": "middle"},
                )
            except Exception:
                pass
        if skipped:
            M.info("Listing all skipped files (non-matching regex):")
            try:
                T.out(
                    [{"source": s, "status": "skipped_non_match"} for s in skipped],
                    sort_by="source",
                    column_options={"justify": "left", "vertical": "middle"},
                )
            except Exception:
                pass
        return planned

    def load_and_match_metadata(sync_file_list: list):
        """Simplified metadata matching: for each file and rule, render key and do a direct
        LabKey select_rows(schema, table) with EQUAL filter on the configured field.
        Uses the top-level LabKey connection. Resolves field caption-to-name once per source.
        """
        metadata_sources_cfg = cfg.get("metadata_sources") or []
        metadata_match_cfg = cfg.get("metadata_match") or {}
        if not metadata_sources_cfg or not metadata_match_cfg:
            return [], {}

        # Map sources by name and check connectivity
        src_by_name = {}
        for src in metadata_sources_cfg:
            sname = src.get("name") or src.get("type")
            if (src or {}).get("type") == "labkey":
                _lk_check(src.get("schema", ""), src.get("table", ""), f"metadata source '{sname}'")
            src_by_name[sname] = src

        # Cache column name resolution per (schema, table)
        qmeta_cache: dict[tuple[str, str], dict] = {}
        def resolve_field(schema: str, table: str, field: str) -> str:
            key = (schema, table)
            if key not in qmeta_cache:
                try:
                    qmeta_cache[key] = api.query.get_query(schema, table) or {}
                except Exception:
                    qmeta_cache[key] = {}
            cols = (qmeta_cache[key] or {}).get("columns", [])
            def _norm(s: str) -> str:
                return "" if s is None else re.sub(r"[^A-Za-z0-9]", "", str(s)).lower()
            want = _norm(field)
            for c in cols or []:
                nm = c.get("name")
                cp = c.get("caption")
                if (_norm(nm) == want) or (_norm(cp) == want):
                    return nm or field
            return field

        # Helper to extract a field value from a row using preferred keys with fallbacks
        def _get_row_value(row: dict, preferred_keys: list[str]):
            for key in preferred_keys:
                if key in row and row.get(key) is not None:
                    return row.get(key)
            # normalized match
            def _norm(s: str) -> str:
                return "" if s is None else re.sub(r"[^A-Za-z0-9]", "", str(s)).lower()
            for key in preferred_keys:
                nk = _norm(key)
                for rk in row.keys():
                    if _norm(rk) == nk and row.get(rk) is not None:
                        return row.get(rk)
            # suffix match as last resort
            for key in preferred_keys:
                nk = _norm(key)
                for rk in row.keys():
                    if _norm(rk).endswith(nk) and row.get(rk) is not None:
                        return row.get(rk)
            return None

        # Perform direct lookups
        default_key_tmpl = metadata_match_cfg.get("key_template")
        rules = metadata_match_cfg.get("search") or []
        for sf in sync_file_list:
            sf["meta_found"], sf["meta_source"], sf["meta_key"] = False, "", ""
            vars_map = sf.get("vars", {})
            for rule in rules:
                src_name, field = rule.get("source"), rule.get("field")
                key_tmpl = rule.get("key_template", default_key_tmpl)
                if not (src_name and field and key_tmpl):
                    continue
                # Render key
                key_val = key_tmpl
                for part, value in vars_map.items():
                    key_val = re.sub(f"<{part}>", str(value), key_val)
                key_val = str(key_val).strip()
                sf["meta_key"] = key_val
                # Resolve source
                src_cfg = src_by_name.get(src_name)
                if not src_cfg or (src_cfg.get("type") != "labkey"):
                    M.warn(f"Metadata source '{src_name}' not available; skipping rule")
                    continue
                schema = src_cfg.get("schema")
                table = src_cfg.get("table")
                rfield = resolve_field(schema, table, field)
                # Direct query
                try:
                    flt = [QueryFilter(rfield, key_val, QueryFilter.Types.EQUAL)]
                    result = api.query.select_rows(schema, table, filter_array=flt)
                    rows = result.get("rows", [])
                    # Strict client-side equality on intended field to avoid wrong matches
                    candidates = []
                    for row in rows or []:
                        v = _get_row_value(row, [rfield, field])
                        if v is not None and str(v).strip() == key_val:
                            candidates.append(row)
                    if len(candidates) > 1:
                        # Ambiguous result: fail fast with context
                        M.error(
                            f"Ambiguous metadata match: source='{src_name}', field='{field}', key='{key_val}'. {len(candidates)} rows matched."
                        )
                        try:
                            rows_sum = []
                            for r in candidates:
                                val = _get_row_value(r, [rfield, field])
                                rows_sum.append({
                                    "RowId": r.get("RowId") or r.get("rowid") or r.get("id"),
                                    "Name": r.get("Name") or r.get("name"),
                                    "value": str(val),
                                })
                            T.out(rows_sum, column_options={"justify": "left", "vertical": "middle"})
                        except Exception:
                            pass
                        exit(2)
                    if candidates:
                        row = candidates[0]
                        meta_rows = sf.setdefault("meta_rows", {})
                        if src_name not in meta_rows:
                            meta_rows[src_name] = row
                        if not sf["meta_found"]:
                            sf["meta_found"], sf["meta_source"], sf["meta_row"] = True, src_name, row
                        # Found a match; stop at first successful rule for this file
                        break
                except Exception as ex:
                    M.warn(f"LabKey lookup failed for {schema}.{table}.{rfield} == '{key_val}': {ex}")
                    continue
        # Summary
        total = len(sync_file_list)
        matched = sum(1 for sf in sync_file_list if sf.get("meta_found"))
        unmatched = total - matched
        M.info(f"Metadata match summary: matched {matched}/{total} ({unmatched} unmatched)")
        return [], {}

    def derive_and_finalize_targets(sync_file_list: list):
        metadata_derive_cfg = cfg.get("metadata_derive")
        _repl_cfg = (cfg.get("replacements") or {})
        before_match_repls = _repl_cfg.get("before_match") or []
        # Helper to read a value from a LabKey row tolerating caption vs name and nested captions
        def _get_row_value(row: dict, field: str):
            if field in row:
                return row.get(field)
            nf = re.sub(r"[^A-Za-z0-9]", "", str(field)).lower()
            # Exact normalized match
            for k in row.keys():
                if re.sub(r"[^A-Za-z0-9]", "", str(k)).lower() == nf:
                    return row.get(k)
            # Suffix normalized match (handles paths like Ancestors/.../Hospital_Secondary_Sample_Id)
            for k in row.keys():
                nk = re.sub(r"[^A-Za-z0-9]", "", str(k)).lower()
                if nk.endswith(nf):
                    return row.get(k)
            return None
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
                    value = _get_row_value(row, field)
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
                        # expose <run> for write-back rendering
                        vars_map = sf.setdefault("vars", {})
                        try:
                            vars_map["run"] = str(run)
                        except Exception:
                            pass
                        break
                    run += 1
            elif filename_sequence == "hash":
                crc = Hasher.crc32(sf["source"])
                candidate = re.sub("<hash>", crc, tmpl)
                sf["target"] = join(repository_folder, candidate)
            else:
                sf["target"] = join(repository_folder, tmpl)
        # Preserve variables; keep meta_rows/meta_row for write-back (no meta_for_write)
        for sf in sync_file_list:
            if "tmpl_vars" not in sf:
                sf["tmpl_vars"] = dict(sf.get("vars") or {})
            sf.pop("vars", None)
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
        presence_cfg = cfg.get("presence_check") or {}
        match_field_cfg = presence_cfg.get("field")  # e.g., "Name" or "file_list"
        match_mode = (presence_cfg.get("match") or "contains").lower()  # "equal" or "contains"

        # Helper to resolve field caption->name for the write-back table
        _wb_cols_cache = None
        def _resolve_wb_field(field: str) -> str:
            nonlocal _wb_cols_cache
            if not field:
                return field
            if _wb_cols_cache is None:
                try:
                    meta = api.query.get_query(labkey["schema"], labkey["table"]) or {}
                    _wb_cols_cache = meta.get("columns", [])
                except Exception:
                    _wb_cols_cache = []
            def _norm(s: str) -> str:
                return "" if s is None else re.sub(r"[^A-Za-z0-9]", "", str(s)).lower()
            want = _norm(field)
            for c in _wb_cols_cache or []:
                nm, cp = c.get("name"), c.get("caption")
                if (_norm(nm) == want) or (_norm(cp) == want):
                    return nm or field
            return field

        for sf in sync_file_list:
            try:
                # Local presence check: does the target file already exist?
                sf["target_exists"] = isfile(sf.get("target", ""))
                # Default: use file_list field with CONTAINS on planned target path
                if not match_field_cfg or str(match_field_cfg).lower() == "file_list":
                    filters = [QueryFilter(file_list_field, sf["target"], QueryFilter.Types.CONTAINS)]
                    sf["presence_field"], sf["presence_value"] = file_list_field, sf.get("target", "")
                else:
                    # Render the chosen field's value from fields config
                    if not isinstance(fields, dict) or match_field_cfg not in fields:
                        M.warn(f"presence_check.field '{match_field_cfg}' not found in fields; falling back to file_list")
                        filters = [QueryFilter(file_list_field, sf["target"], QueryFilter.Types.CONTAINS)]
                        sf["presence_field"], sf["presence_value"] = file_list_field, sf.get("target", "")
                    else:
                        field_template = fields.get(match_field_cfg)
                        rendered_val = _render_value(sf, dict(sf.get("tmpl_vars") or {}), str(field_template))
                        rfield = _resolve_wb_field(match_field_cfg)
                        if match_mode == "equal":
                            filters = [QueryFilter(rfield, rendered_val, QueryFilter.Types.EQUAL)]
                        else:
                            filters = [QueryFilter(rfield, rendered_val, QueryFilter.Types.CONTAINS)]
                        sf["presence_field"], sf["presence_value"] = rfield, rendered_val

                results = api.query.select_rows(labkey["schema"], labkey["table"], filter_array=filters)
                rows = results.get("rows", [])
                sf["in_labkey"] = len(rows) > 0
                if sf["in_labkey"]:
                    sf["existing_row"] = rows[0]
            except RequestError as ex:
                M.error("Labkey request error:")
                M.error(ex)
        return sync_file_list

    def render_plan_table(rows: list):
        # Drop verbose fields from display
        display_rows = []
        for r in rows:
            if isinstance(r, dict):
                r2 = dict(r)
                r2.pop("meta_for_write", None)
                # Compute JSON view of matched meta row for display
                try:
                    _meta_obj = r.get("meta_row")
                    if not _meta_obj:
                        _mrs = r.get("meta_rows") or {}
                        if _mrs:
                            # Prefer primary source if available
                            _src = r.get("meta_source")
                            _meta_obj = _mrs.get(_src) if _src in _mrs else next(iter(_mrs.values()), None)
                    r2["meta_row_json"] = json.dumps(_meta_obj, ensure_ascii=False) if _meta_obj else ""
                except Exception:
                    r2["meta_row_json"] = ""
                # Hide heavy internals
                r2.pop("existing_row", None)
                r2.pop("meta_rows", None)
                r2.pop("meta_row", None)
                r2.pop("presence_field", None)
                r2.pop("presence_value", None)
                # Indicate if this will update an existing LabKey row or create a new one
                r2["write_action"] = "update" if r2.get("in_labkey") else "create"
                display_rows.append(r2)
            else:
                display_rows.append(r)
        T.out(
            display_rows,
            sort_by="source",
            row_style=lambda row: (
                "red"
                if (row.get("meta_found") is False)
                else (
                    "red"
                    if row.get("integrity_method") == "md5" and (row.get("md5_ok") is False)
                    else None
                )
            ),
        )

    def summarize_copy_plan(sync_file_list: list, metadata_required: bool):
        """Print a concise summary of where files would be copied in dry-run mode."""
        rows = []
        for sf in sync_file_list:
            action = "would_copy"
            reason = ""
            if not sf.get("meta_found", False):
                action, reason = "would_skip", "metadata_missing"
            elif sf.get("integrity_method") == "md5" and (sf.get("md5_ok") is False):
                action, reason = "would_skip", "md5_mismatch"
            rows.append({
                "source": sf.get("source", ""),
                "target": sf.get("target", ""),
                "target_exists": bool(sf.get("target_exists", False)),
                "action": action if not reason else f"{action}:{reason}",
            })
        M.info("Copy plan summary (dry run):")
        T.out(
            rows,
            sort_by="source",
            column_options={"justify": "left", "vertical": "middle"},
            row_style=lambda r: (
                "red"
                if (isinstance(r, dict) and str(r.get("action", "")).startswith("would_skip"))
                else None
            ),
        )

    def perform_copy_and_writeback(sync_file_list: list):
        synced_file_list = []
        writeback_rows = []
        update_diff_rows = []
        create_field_groups: dict[tuple[str, str], list] = {}
        # Helper to resolve write-back field name and read existing values
        _wb_cols_cache = None
        def _resolve_wb_field_for_updates(field: str) -> str:
            nonlocal _wb_cols_cache
            if not field:
                return field
            if _wb_cols_cache is None:
                try:
                    meta = api.query.get_query(labkey["schema"], labkey["table"]) or {}
                    _wb_cols_cache = meta.get("columns", [])
                except Exception:
                    _wb_cols_cache = []
            def _norm(s: str) -> str:
                return "" if s is None else re.sub(r"[^A-Za-z0-9]", "", str(s)).lower()
            want = _norm(field)
            for c in _wb_cols_cache or []:
                nm, cp = c.get("name"), c.get("caption")
                if (_norm(nm) == want) or (_norm(cp) == want):
                    return nm or field
            return field
        for sync_file in sync_file_list:
            # Copy source to target
            copy_ok = False
            target_exists_before = isfile(sync_file.get("target", ""))
            # Always require metadata match before copying
            if not sync_file.get("meta_found", False):
                M.warn(f"Skipping {sync_file['source']} due to missing metadata (metadata_required).")
                synced_file_list.append({
                    "source": sync_file["source"],
                    "target": sync_file["target"],
                    "copy_ok": False,
                    "verified": False,
                    "reason": "metadata_missing",
                    "sidecar": "",
                    "sidecar_copy_ok": False,
                    "target_exists_before": target_exists_before,
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
                            "target_exists_before": target_exists_before,
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
                "target_exists_before": target_exists_before,
            })
            # Prepare LabKey row for write-back when verified
            if verified and fields and isinstance(fields, dict):
                # Load replacements config for write-back
                _repl_cfg = (cfg.get("replacements") or {})
                before_wb_repls = _repl_cfg.get("before_writeback") or []
                planned_row = _build_row(sync_file, fields, before_wb_repls)
                writeback_rows.append(planned_row)
                # If presence indicates existing row, log what would change
                if sync_file.get("meta_found") and sync_file.get("in_labkey") and isinstance(sync_file.get("existing_row"), dict):
                    existing = sync_file.get("existing_row") or {}
                    key_field = sync_file.get("presence_field") or ""
                    key_value = sync_file.get("presence_value") or ""
                    for k in (fields or {}).keys():
                        rfield = _resolve_wb_field_for_updates(k)
                        old_val = existing.get(rfield)
                        new_val = planned_row.get(k)
                        changed = str(old_val) != str(new_val)
                        update_diff_rows.append({
                            "row_key_field": key_field,
                            "row_key_value": key_value,
                            "field": k,
                            "from": str(old_val),
                            "to": str(new_val),
                            "will_change": "yes" if changed else "no",
                        })
                elif sync_file.get("meta_found") and not sync_file.get("in_labkey"):
                    # Collect create fields per planned key
                    key_field = sync_file.get("presence_field") or ""
                    key_value = sync_file.get("presence_value") or ""
                    group_key = (str(key_field), str(key_value))
                    rows = create_field_groups.setdefault(group_key, [])
                    for k in (fields or {}).keys():
                        rows.append({"field": k, "to": str(planned_row.get(k))})

        T.out(
            synced_file_list,
            sort_by="source",
            column_options={"justify": "left", "vertical": "middle"},
            row_style=lambda row: (
                "red"
                if (not row.get("copy_ok"))
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

        # End-of-run copy summary
        summary_rows = []
        for r in synced_file_list:
            action = "copied" if (r.get("copy_ok") and r.get("verified")) else f"skipped:{r.get('reason','')}"
            summary_rows.append({
                "source": r.get("source", ""),
                "target": r.get("target", ""),
                "target_exists_before": bool(r.get("target_exists_before", False)),
                "action": action,
            })
        M.info("Copy summary (executed):")
        T.out(
            summary_rows,
            sort_by="source",
            column_options={"justify": "left", "vertical": "middle"},
            row_style=lambda r: (
                "red" if (isinstance(r, dict) and str(r.get("action", "")).startswith("skipped")) else None
            ),
        )
        # Log planned update changes (executed mode), one table per matched row
        if update_diff_rows:
            try:
                T.console.rule("Planned update changes (executed run)")
            except Exception:
                pass
            # Group by (row_key_field, row_key_value)
            groups: dict[tuple[str, str], list] = {}
            for d in update_diff_rows:
                key = (str(d.get("row_key_field", "")), str(d.get("row_key_value", "")))
                groups.setdefault(key, []).append({
                    "field": d.get("field", ""),
                    "from": d.get("from", ""),
                    "to": d.get("to", ""),
                    "will_change": d.get("will_change", ""),
                })
            for (kf, kv), rows in groups.items():
                M.info(f"Planned update changes (executed run) for {kf} == '{kv}':")
                T.out(
                    rows,
                    sort_by="field",
                    column_options={"justify": "left", "vertical": "middle"},
                    row_style=lambda r: ("yellow" if (isinstance(r, dict) and str(r.get("will_change", "")).lower() in ("yes", "true", "1")) else None),
                )

        # Log planned create fields (executed mode), one table per new row
        if create_field_groups:
            try:
                T.console.rule("Planned create fields (executed run)")
            except Exception:
                pass
            for (kf, kv), rows in create_field_groups.items():
                M.info(f"Planned create fields (executed run) for {kf} == '{kv}':")
                T.out(rows, column_options={"justify": "left", "vertical": "middle"})
        
        if writeback_rows:
            try:
                api.query.insert_rows(labkey["schema"], labkey["table"], writeback_rows)
                M.info(f"Inserted {len(writeback_rows)} row(s) into LabKey {labkey['schema']}.{labkey['table']}")
            except Exception as ex:
                M.error(ex)
        return

    def dry_run_writeback(sync_file_list: list):
        if not (fields and isinstance(fields, dict)):
            return
        writeback_rows = []
        _repl_cfg = (cfg.get("replacements") or {})
        before_wb_repls = _repl_cfg.get("before_writeback") or []
        # Helpers for diffing
        _wb_cols_cache = None
        def _resolve_wb_field_for_updates(field: str) -> str:
            nonlocal _wb_cols_cache
            if not field:
                return field
            if _wb_cols_cache is None:
                try:
                    meta = api.query.get_query(labkey["schema"], labkey["table"]) or {}
                    _wb_cols_cache = meta.get("columns", [])
                except Exception:
                    _wb_cols_cache = []
            def _norm(s: str) -> str:
                return "" if s is None else re.sub(r"[^A-Za-z0-9]", "", str(s)).lower()
            want = _norm(field)
            for c in _wb_cols_cache or []:
                nm, cp = c.get("name"), c.get("caption")
                if (_norm(nm) == want) or (_norm(cp) == want):
                    return nm or field
            return field
        update_diff_rows = []
        create_field_groups: dict[tuple[str, str], list] = {}
        for sf in sync_file_list:
            planned_row = _build_row(sf, fields, before_wb_repls)
            writeback_rows.append(planned_row)
            if sf.get("meta_found") and sf.get("in_labkey") and isinstance(sf.get("existing_row"), dict):
                existing = sf.get("existing_row") or {}
                key_field = sf.get("presence_field") or ""
                key_value = sf.get("presence_value") or ""
                for k in (fields or {}).keys():
                    rfield = _resolve_wb_field_for_updates(k)
                    old_val = existing.get(rfield)
                    new_val = planned_row.get(k)
                    changed = str(old_val) != str(new_val)
                    update_diff_rows.append({
                        "row_key_field": key_field,
                        "row_key_value": key_value,
                        "field": k,
                        "from": str(old_val),
                        "to": str(new_val),
                        "will_change": "yes" if changed else "no",
                    })
            elif sf.get("meta_found"):
                # Will be created: show planned fields per row key
                key_field = sf.get("presence_field") or ""
                key_value = sf.get("presence_value") or ""
                group_key = (str(key_field), str(key_value))
                rows = create_field_groups.setdefault(group_key, [])
                for k in (fields or {}).keys():
                    rows.append({"field": k, "to": str(planned_row.get(k))})
        # No JSON dump of planned rows; show per-row create tables and update diffs instead
        if update_diff_rows:
            # Group by (row_key_field, row_key_value)
            groups: dict[tuple[str, str], list] = {}
            for d in update_diff_rows:
                key = (str(d.get("row_key_field", "")), str(d.get("row_key_value", "")))
                groups.setdefault(key, []).append({
                    "field": d.get("field", ""),
                    "from": d.get("from", ""),
                    "to": d.get("to", ""),
                    "will_change": d.get("will_change", ""),
                })
            for (kf, kv), rows in groups.items():
                M.info(f"Planned update changes (dry run) for {kf} == '{kv}':")
                T.out(
                    rows,
                    sort_by="field",
                    column_options={"justify": "left", "vertical": "middle"},
                    row_style=lambda r: ("yellow" if (isinstance(r, dict) and str(r.get("will_change", "")).lower() in ("yes", "true", "1")) else None),
                )
        if create_field_groups:
            try:
                T.console.rule("Planned create fields (dry run)")
            except Exception:
                pass
            for (kf, kv), rows in create_field_groups.items():
                M.info(f"Planned create fields (dry run) for {kf} == '{kv}':")
                T.out(rows, column_options={"justify": "left", "vertical": "middle"})
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
            row = (sync_file.get("meta_rows") or {}).get(src_name)
            if row is None and sync_file.get("meta_source") == src_name:
                row = sync_file.get("meta_row")
            if row is None:
                return ""
            # Try direct key, then normalized match, then suffix match
            if field_name in row and (row[field_name] is not None):
                return str(row[field_name])
            nf = re.sub(r"[^A-Za-z0-9]", "", field_name).lower()
            for k in row.keys():
                if re.sub(r"[^A-Za-z0-9]", "", str(k)).lower() == nf and row.get(k) is not None:
                    return str(row.get(k))
            for k in row.keys():
                nk = re.sub(r"[^A-Za-z0-9]", "", str(k)).lower()
                if nk.endswith(nf) and row.get(k) is not None:
                    return str(row.get(k))
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
        # Auto-fill with tolerant labels (spaces vs underscores)
        if any(k in fields_cfg for k in ("Path_To_Synced_Data", "Path To Synced Data")):
            key = "Path_To_Synced_Data" if "Path_To_Synced_Data" in fields_cfg else "Path To Synced Data"
            row[key] = sync_file["target"]
        if any(k in fields_cfg for k in ("Uploaded_File_Name", "Uploaded File Name")):
            key = "Uploaded_File_Name" if "Uploaded_File_Name" in fields_cfg else "Uploaded File Name"
            row[key] = basename(sync_file["source"])
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
        summarize_copy_plan(sync_file_list, bool(cfg.get("metadata_required", False)))
if __name__ == "__main__":
    cli()
