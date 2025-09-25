# Nexus Data Management Tools

A small Python CLI for synchronizing data files from a "drop" directory into a canonical repository structure, validating integrity (checksums), and preparing metadata for recording in a LabKey server. Configuration is merged from a global config and a per-drop-folder config to allow site-specific overrides.

---

## Table of Contents

- Overview
- Features
- Architecture and Key Files
- Installation
- Configuration
- Usage
  - sync
  - check
- How It Works (Detailed Flow)
- Output and Tables
- Known Limitations and Gaps
- Roadmap / Next Steps
- Development
- Troubleshooting
- License

---

## Overview

This tool automates moving files discovered in a "drop" directory into a structured repository path. It uses a filename filter and a strict regular expression with named groups to parse metadata from filenames, computes checksums, checks if the files are already present in LabKey, and optionally copies the files.

The tool prepares row data for LabKey insertion, including optional aggregation of multiple files into a single record keyed by a user-specified field. In the current version, inserting/updating rows in LabKey and moving files into a processed area are planned but not yet implemented (see Roadmap).

---

## Features

- Discovery of input files via glob pattern (e.g., `**/*.fastq.gz`) and a strict regex with named capture groups.
- Construction of target repository path using placeholders like `<phase>`, `<lib>`, `<seq>`, `<run>`, `<hash>`.
- Filename de-duplication via configurable sequence number (run) or deterministic short hash.
- MD5 checksum computation of source files; optional validation against sidecar `.md5` files.
- LabKey presence check for the target path using the `labkey` Python SDK.
- Tabular console output using `rich`, with colored styling for quick status review.
- Config layering: global `dm/sync.yml` is deep-merged with per-drop-folder `sync.yml`.

---

## Architecture and Key Files

- `dm/dm.py`
  - `sync`: main command that performs discovery, validation, path rendering, checksum verification, LabKey query, and optional copying.
  - `check`: simple command to verify LabKey connectivity.
- `dm/config.py`
  - `options_from_source()`: `click` decorator that merges configuration from:
    - Global: `dm/sync.yml`
    - Local: `<drop_folder>/sync.yml`
- `dm/functions.py`
  - `now(date_format)`: returns current local time string in given format.
  - `drop_file_mtime(filename, date_format)`: returns mtime of a file.
- `dm/helpers.py`
  - `Message`: colored console messages.
  - `TableOutput`: pretty table printer using `rich`.
  - `Hasher`: `crc32` and `md5` helpers.
  - `Executor`: shell execution helper (currently unused).
- `dm/sync.yml`
  - Example/global configuration with defaults for sync operation and LabKey mapping.
- `environment.yml`
  - Conda environment specification (Python 3.10 + pip dependencies).
- `Makefile`
  - `envupdate`: update the conda environment from `environment.yml`.
- `.vscode/settings.json`
  - Editor settings for formatting with Black.

---

## Installation

Recommended with conda or mamba.

1) Create/update environment with Makefile:

```bash
make envupdate
```

2) Or manually with conda/mamba:

```bash
# Recommended: create a new environment named dm
yes | conda env create -f environment.yml -n dm || conda env update -f environment.yml -n dm --prune
conda activate dm
```

Note:
- `environment.yml` currently contains a `prefix:` pointing to a local path. You can remove that line or override the environment name with `-n dm` as shown above.

3) Verify dependencies (installed via pip in the environment):
- `pyyaml`, `labkey`, `yachalk`, `rich`, `click`, `python-dotenv`, `deepmerge`

---

## Configuration

Configuration is loaded and deep-merged from two YAML files:

- Global: `dm/sync.yml` (shipped in this repo)
- Local: `<drop_folder>/sync.yml` (must exist in the drop folder you pass via `--drop-folder`)

The local drop-folder config overrides values from the global config.

Key settings in `sync.yml`:

- `drop_folder`: Path to the drop directory (only used in examples; you pass this via CLI).
- `drop_filename_filter`: Glob pattern to find files (e.g., `**/*.fastq.gz`).
- `drop_filename_regex`: Regex with named groups to parse metadata from file paths. Example groups used: `phase`, `seq`, `prefix`, `lib`, `suffix`.
- `repository_folder`: Root folder where files are to be synchronized.
- `repository_filename`: Template for the target filename, e.g., `scRNA/raw/<phase>/<lib>/<lib>_<seq>_r<run>__<suffix>.fastq.gz`. Placeholders are replaced using the named groups and special values.
- `processed_folder`: Root where processed (or archived) drop files should be moved. (Not yet implemented)
- `filename_sequence`: Either `run` (increments `<run>`) or `hash` (sets `<hash>` to a short CRC32-derived value).
- `date_format`: Datetime format string used by functions like `now()` or `drop_file_mtime()`.
- `labkey`:
  - `host`: LabKey host
  - `container`: LabKey container/folder (e.g., `LOOP Intercept`)
  - `schema`: Target schema (e.g., `exp`) — see Known Limitations for a note
  - `table`: Target table (e.g., `data` or `scRNA_Experiments`) — see Known Limitations
  - `context`: Optional; not currently used.
- `fields`: Mapping of LabKey field names to values. Supports:
  - Placeholders like `<phase>`, `<lib>`, `<seq>`, `<run>`, `<hash>`, `<prefix>`, `<suffix>`
  - Functions: `now()`, `drop_file_mtime()`
  - Literal values (booleans, numbers, strings)
- `field_parameters`:
  - `file_list`: which field is considered the “file list” (e.g., `Path_To_Synced_Data`)
  - `file_list_aggregator`: the field used to group/aggregate file lists (e.g., `Uploaded_Filename_Prefix`)
- `lookups`:
  - Optional value translation mapping, e.g., mapping `phase: btki -> BTKi`.

Example global `dm/sync.yml` (excerpt):

```yaml
drop_filename_regex: "[^\/]+\/(?P<phase>[^\/]+)\/.*(?P<seq>SEQ_[A-Z]{5}).*(?P<prefix>(?P<lib>[A-Z]{3}_[A-Z]{6})_[A-Z][0-9]+_[A-Z][0-9]{3})_(?P<suffix>[^.]+)"
drop_filename_filter: "**/*.fastq.gz"
repository_folder: data/repository
repository_filename: scRNA/raw/<phase>/<lib>/<lib>_<seq>_r<run>__<suffix>.fastq.gz
processed_folder: data/processed
filename_sequence: run
labkey:
  host: your-labkey-host.example.org
  container: LOOP Intercept
  schema: exp
  table: data
fields:
  Name: <lib>_<seq>_r<run>
  Project_Phase: <phase>
  Run_Number: <run>
  Uploaded_Filename_Prefix: <prefix>
  Data_synced: true
  Date_Of_Syncing: now()
  Date_Of_Uploading: drop_file_mtime()
  Path_To_Synced_Data:
field_parameters:
  Path_To_Synced_Data: file_list
  Uploaded_Filename_Prefix: file_list_aggregator
lookups:
  phase:
    btki: BTKi
```

---

## Usage

Run commands from the repo root unless otherwise noted.

### sync

Synchronize files from a drop folder into the repository.

Typical usage (with configuration taken from `<drop_folder>/sync.yml`):

```bash
python dm/dm.py sync --drop-folder /path/to/drop
```

Options exist for overriding specific values on the command line, but the intended pattern is to keep most settings in YAML. If you want to perform the copy operation, pass `--do-it` and confirm when prompted:

```bash
python dm/dm.py sync --drop-folder /path/to/drop --do-it
```

Notes:
- The tool enforces the regex; if a file doesn’t match `drop_filename_regex`, it exits with an error.
- `<run>` increments to avoid collisions within a run when `filename_sequence: run`. If `hash` mode is enabled, `<hash>` is set to a CRC32-derived value of the source file.
- Pre-copy integrity logic:
  - If `.md5` sidecar is present: MD5 is checked; mismatches prevent copy.
  - If `.md5` sidecar is absent: a `.blake3` sidecar is created for the source before copying.

### check

Validate LabKey connectivity and basic query capability:

```bash
python dm/dm.py check -h your-labkey-host.example.org -c "Your Container" -s exp -t data
```

The command prints the result of a simple `select_rows`.

---

## How It Works (Detailed Flow)

Inside `dm/dm.py` (`sync` command):

1. Configuration merge: Using `options_from_source("--drop-folder")` from `dm/config.py`, the tool deep-merges `dm/sync.yml` with `<drop_folder>/sync.yml` (local overrides global).
2. File discovery: Uses `glob` with `drop_filename_filter` to find candidate files in the drop folder.
3. Regex validation and capture: Validates each file path against `drop_filename_regex`; extracts named groups (e.g., `phase`, `seq`, `lib`, `prefix`, `suffix`).
4. Target filename rendering: Renders `repository_filename` by replacing placeholders. Resolves `<run>` collision or sets `<hash>` depending on `filename_sequence`.
5. Prepare LabKey rows: Builds one row per file by applying:
   - Functions: `now()`, `drop_file_mtime()` from `dm/functions.py`
   - Placeholder substitution
   - Value translation with `lookups`
   - Sets the file-list field (e.g., `Path_To_Synced_Data`) to the final target path
6. Optional aggregation: Aggregates rows by `file_list_aggregator` (e.g., `Uploaded_Filename_Prefix`) by concatenating the file-list into a comma-separated string. This aggregated set is computed but not yet used for insertion.
7. Pre-copy integrity:
   - If a sidecar `.md5` exists: compute the file's MD5 (streamed) and compare to the sidecar value. Files with a mismatch are flagged and will be skipped during copy.
   - If no sidecar `.md5` exists: the tool will compute a BLAKE3 digest and write a `.blake3` sidecar for the source file before copying.
8. LabKey presence check: Queries LabKey (`QueryFilter` with `CONTAINS` on `Path_To_Synced_Data`) to set `in_labkey=True/False` for each planned target file.
9. Reporting: Prints a table of planned sync actions with color cues.
10. Copy (if `--do-it`):
    - If `.md5` sidecar exists and matches: proceed to copy.
    - If `.md5` sidecar exists and mismatches: skip copy and report an error.
    - If no `.md5` sidecar exists: compute and write a `.blake3` sidecar for the source file, then copy.
    - After copy: verify by block-by-block compare. If verification succeeds, copy the matching sidecar (`.md5` or `.blake3`) to the repository alongside the main file. Then print a second table showing copy status.

---

## Output and Tables

Two tables are printed using `rich`:

- Pre-copy planning table (integrity and presence checks):
  - `source`: Absolute path of source file in the drop folder
  - `target`: Target path in the repository
  - `md5`: MD5 of the source file
  - `orig_md5`: MD5 read from sidecar file (if any)
  - `md5_ok`: Whether `md5 == orig_md5`
  - `in_labkey`: Whether LabKey already has a row referencing the target path

- Post-copy table (copy and verification status):
  - `source`
  - `target`
  - `copy_ok`: Whether the copy operation created the target file
  - `verified`: Result of block-by-block comparison between source and target
  - `sidecar`: Which sidecar was used/copied (`md5`, `blake3`, or empty if none)
  - `sidecar_copy_ok`: Whether sidecar copy to the target location succeeded

Color highlighting:
- Pre-copy: Red rows indicate `md5_ok == False` (currently marks as failing if no `.md5` file is present — see Known Limitations).
- Post-copy: Yellow rows indicate `copy_ok == False`; Red rows indicate `verified == False` or a sidecar was expected/copied (`md5`/`blake3`) but `sidecar_copy_ok == False`.

---

## Known Limitations and Gaps

- LabKey insert/update not implemented: The tool prepares rows (and an aggregated variant) but does not perform `insert_rows`/`upsert_rows`.
- Move to processed not implemented: `processed_folder` is defined but not used yet to move files after a successful sync.
- Aggregation is unused: `aggregated_rows` are built but not used for any downstream operation.
- MD5 policy is strict: If no sidecar `.md5` exists, `md5_ok` is set to `False` (red). Consider treating this as “not checked” rather than a failure.
- Potential config mismatch: In `dm/sync.yml`, `schema: exp.data` and `table: scRNA_Experiments` may be inconsistent with how `select_rows(schema, table)` is called. Typically, use `schema: exp` with `table: data` OR a custom `schema` with `table: scRNA_Experiments`.
- Unused `labkey.context`: The config contains `context` but it is not passed to `APIWrapper`.
- Type coercion: Non-function field values are currently cast to strings before being added to rows; this may lead to type mismatches in LabKey.
- Import style: `dm/dm.py` uses non-relative imports (`import functions`, `from helpers import ...`). Running as a module (`python -m dm.dm`) may fail. Consider switching to relative imports.
- Large file hashing: `Hasher.md5`/`crc32` read whole files into memory; swap to streaming for large files.
- Minor repo hygiene:
  - `.gitignore`: `__pychache__/` typo should be `__pycache__/`.
  - `environment.yml` has a hard-coded `prefix:`; remove for portability.
  - `requirements.dev.txt` only contains `pur`.

---

## Roadmap / Next Steps

- Implement LabKey `insert_rows`/`upsert_rows` with retries and clear error handling.
- Decide whether to insert per-file rows or aggregated rows keyed by `file_list_aggregator`.
- Implement moving successfully processed drop files into `processed_folder`, preserving structure.
- Relax MD5 policy: Treat missing `.md5` as “not checked” while still verifying that target MD5 equals source MD5.
- Preserve data types in `fields` (e.g., booleans, numbers) instead of casting to strings.
- Switch to package-relative imports and support running via `python -m dm.dm`.
- Stream-based hashing for large files.
- Add unit tests for config merge, path rendering, and basic flows.
- Add CI, expand documentation, and provide more examples.

---

## Development

- Code style: Black and Flake8 are configured in `setup.cfg`. VS Code will auto-format on save per `.vscode/settings.json`.
- Update environment:

```bash
make envupdate
conda activate dm
```

- Run commands:

```bash
# Dry-run sync using config in the drop folder
echo "See planned actions"
python dm/dm.py sync --drop-folder /path/to/drop

# Execute copy after confirmation
python dm/dm.py sync --drop-folder /path/to/drop --do-it

# LabKey connectivity check
python dm/dm.py check -h your-labkey-host -c "Your Container" -s exp -t data
```

---

## Troubleshooting

- Regex mismatch → program exits with error: Ensure `drop_filename_regex` matches the discovered file paths.
- LabKey errors: Review host/container/schema/table and your credentials/permissions. The tool catches and prints errors like `ServerContextError`, `ServerNotFoundError`, `QueryNotFoundError`, and `RequestError`.
- No `.md5` sidecar: Current behavior flags `md5_ok == False`; this will be improved (see Roadmap).
- Config precedence: Values from `<drop_folder>/sync.yml` override `dm/sync.yml`.
- Running as a module: Current imports are script-relative; running `python -m dm.dm` may fail until imports are made relative.

---

## License

TBD
