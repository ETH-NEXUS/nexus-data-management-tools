# Nexus Data Management Tools

A small Python CLI for synchronizing data files from a "drop" directory into a canonical repository structure, validating integrity (checksums), and preparing metadata for recording in a LabKey server. Configuration is loaded solely from a per-drop-folder `sync.yml` to allow site-specific behavior.

---

## Table of Contents

- Overview
- Features
- Architecture and Key Files
- Installation
- Quickstart
- Configuration
- Usage
  - sync
- How It Works (Detailed Flow)
- Output and Tables
- Known Limitations and Gaps
- Roadmap / Next Steps
- Development
- Troubleshooting
- License

---

## Overview

This tool automates synchronizing files discovered in a "drop" directory into a structured repository path. It uses a filename filter and a strict regular expression with named groups to parse metadata from filenames, computes checksums, performs a presence check in LabKey, and copies files when run in execute mode. It can update existing LabKey rows (and optionally insert new ones) based on a configurable mapping.

After a successful and verified copy to the repository, the original drop files can be moved (archived) into a configured processed folder, preserving the subdirectory structure relative to the drop folder. Both copy and move actions are only executed when `--do-it` is provided; otherwise, the tool prints detailed dry-run plans.

---

## Features

- **[Discovery and templating]** Glob discovery (`drop_filename_filter`) and strict regex parsing (`drop_filename_regex`) with named capture groups.
- **[Target rendering]** `repository_filename` supports placeholders: regex groups, `<run>` (auto-increment), `<hash>` (CRC32), and `<source.Field>` from matched metadata.
- **[Integrity]** If a `.md5` sidecar exists, verify before copying; else compute a BLAKE3 digest and write a `.blake3` sidecar pre-copy. Post-copy verification is a block-by-block compare.
- **[LabKey presence + write-back]** Presence check via `field_parameters.file_list` (default CONTAINS by target path) or a configured presence field with `equal|contains` semantics. In execute mode, updates existing rows and optionally inserts new ones unless `writeback.skip_creates: true`.
- **[Skip creates]** When `skip_creates: true`, planned creates are suppressed in logs; copy and move are also skipped for files where write-back is skipped (including existing rows without a `RowId`).
- **[Dry-run plans]**
  - Pre-run plan table for each file (including `write_action: update|create|skip_create`).
  - Copy plan summary: `would_copy` or `would_skip:<reason>`.
  - Update diffs per matched row; planned create fields (suppressed when `skip_creates: true`).
  - Archive/move plan summary mirroring copy gating: `would_move` or `would_skip:<reason>`.
- **[Executed run summaries]**
  - Copy summary (copied vs skipped with reasons), update diffs, planned creates (suppressed when `skip_creates: true`).
  - Archive/move summary after verified copies: moves originals under `processed_folder` preserving structure.
- **[Console + Log]** Styled tables via `rich` and a tee’d log file at `dm/logs/sync/<runmode>/<dataset>-<timestamp>.log`.


---

## Architecture and Key Files

- `dm/dm.py`
  - `sync`: main CLI command. Loads `<drop_folder>/sync.yml`, discovers files, renders targets, checks integrity, optional metadata matching, LabKey presence, and outputs dry-run plans. With `--do-it`, performs copy + verify, write-back (update/insert), copies sidecars, and archives originals into `processed_folder`.
- `dm/helpers.py`
  - `Message`, `TableOutput`, `Hasher` utilities.
- `dm/integrity.py`
  - Read `.md5`, write `.blake3`, and copy matching sidecar files.
- `dm/metadata.py`
  - General-purpose metadata loaders (LabKey/Excel/CSV). Note: the current pipeline in `dm/dm.py` performs simplified LabKey-based lookups inline.
- `dm/sync.yml.TEMPLATE`
  - Reference configuration for per-drop `sync.yml` files.
- `environment.yml`, `Makefile`, `.vscode/settings.json`
  - Environment, development helpers, and editor config.

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
This project uses a standard, portable `environment.yml` without a hard-coded `prefix:`.

3) Verify dependencies (installed via pip in the environment):
- `pyyaml`, `labkey`, `yachalk`, `rich`, `click`, `python-dotenv`, `openpyxl`, `blake3`

---

## Quickstart

Follow these steps to try the tool with a minimal configuration.

1) Create a per-drop config at `<drop_folder>/sync.yml`

Minimal config (no metadata usage):

```yaml
# <drop_folder>/sync.yml
drop_filename_filter: "**/*.fastq.gz"
drop_filename_regex: "[^\\/]+\\/(?P<phase>[^\\/]+)\/.*(?P<seq>SEQ_[A-Z]{5}).*(?P<prefix>(?P<lib>[A-Z]{3}_[A-Z]{6})_[A-Z][0-9]+_[A-Z][0-9]{3})_(?P<suffix>[^.]+)"

repository_folder: /cluster/work/nexusintercept/data-repository
repository_filename: scRNA/raw/<phase>/<lib>/<lib>_<seq>_r<run>__<suffix>.fastq.gz
processed_folder: /cluster/work/nexusintercept/data-processed
filename_sequence: run

labkey:
  host: intercept-labkey-dev.nexus.ethz.ch  # set to your LabKey host
  container: "LOOP Intercept"               # set to your LabKey container/folder
  schema: exp                                # adjust to your schema
  table: data                                # adjust to your table

field_parameters:
  Path_To_Synced_Data: file_list
```

2) Run a dry run (planning only), then actually copy with `--do-it`:

```bash
python dm/dm.py sync --drop-folder /path/to/drop
python dm/dm.py sync --drop-folder /path/to/drop --do-it
```

Tiny example: include a field from LabKey in the target path using `<source.Field>`

Add a simple metadata source and match rule, then reference it in `repository_filename`:

```yaml
# Snippet to add to <drop_folder>/sync.yml
metadata_sources:
  - name: lk_experiments
    type: labkey
    host: intercept-labkey-dev.nexus.ethz.ch
    container: "LOOP Intercept"
    schema: exp
    table: data
    columns: [Name, Uploaded_Filename_Prefix]

metadata_match:
  key_template: <prefix>
  search:
    - source: lk_experiments
      field: Uploaded_Filename_Prefix

# Now you can use a metadata placeholder in the filename template
repository_filename: scRNA/raw/<phase>/<lib>/<lk_experiments.Name>_r<run>__<suffix>.fastq.gz
```

Notes:
- Ensure your drop files match the `drop_filename_regex` (update the regex to fit your naming scheme).
- Adjust LabKey `schema` and `table` to your server’s layout. If you use a custom table, set `schema` (e.g., `exp`) and `table` (e.g., `scRNA_Experiments`) accordingly.
- If a `.md5` sidecar exists next to a source file, it will be verified. Otherwise a `.blake3` sidecar is created before copying.

---

## Configuration

Configuration is loaded from a single YAML file located in the drop folder:

- Local: `<drop_folder>/sync.yml` (must exist in the drop folder you pass via `--drop-folder`).

Use `dm/sync.yml.TEMPLATE` as a reference template for creating per-drop sync files. The drop folder path itself is provided only via the CLI option `--drop-folder` and is not included in any YAML.

Key settings in `sync.yml`:

- `drop_filename_filter`: Glob pattern to find files (e.g., `**/*.fastq.gz`).
- `drop_filename_regex`: Regex with named groups to parse metadata from file paths. Example groups used: `phase`, `seq`, `prefix`, `lib`, `suffix`.
- `repository_folder`: Root folder where files are to be synchronized.
- `repository_filename`: Template for the target filename, e.g., `scRNA/raw/<phase>/<lib>/<lib>_<seq>_r<run>__<suffix>.fastq.gz`. Placeholders are replaced using the named groups and special values. Also supports metadata placeholders of the form `<source.Field>` after metadata matching, e.g., `scRNA/raw/<phase>/<lib>/<phase>_<lk_experiments.Name>_r<run>__<suffix>.fastq.gz`.
- `processed_folder`: Parent folder where original drop files are moved after a verified copy. The relative path under the drop folder is preserved.
- `filename_sequence`: Either `run` (increments `<run>`) or `hash` (sets `<hash>` to a short CRC32-derived value).
- `date_format`: Datetime format string intended for use by helper functions like `now()` or `drop_file_mtime()`. Reserved for future LabKey write-back (not used by the core flow yet).
- `labkey`:
  - `host`: LabKey host
  - `container`: LabKey container/folder (e.g., `LOOP mTORUS`)
  - `schema`: Target schema name only (no dots), e.g., `exp` or your custom schema
  - `table`: Target table name in that schema, e.g., `16S_Experiments`
  - `context`: Optional context path; passed through to the API wrapper
- `metadata_sources`: External metadata sources. The current pipeline in `dm/dm.py` performs inline LabKey lookups using the top-level LabKey connection. Other types (Excel/CSV) exist in `dm/metadata.py` but are not invoked by the default pipeline.
- `metadata_match`: Rules to find the metadata row for each file before syncing.
  - `key_template`: Default template string to render a metadata key from the filename regex variables (e.g., `<prefix>`, `<lib>_<seq>`).
  - `search`: Ordered list of rule objects, each with:
    - `source`: Name of the source from `metadata_sources` to search.
    - `field`: Field/column name within that source to match against.
    - `key_template` (optional): Override the default template for this rule.
- `metadata_required`: Boolean flag. If `true`, files without a matching metadata row are skipped (reason `metadata_missing`) for both copy and archive plans.

Example `metadata_sources` configuration:

```yaml
metadata_sources:
  - name: lk_experiments
    type: labkey
    host: your-labkey-host.example.org
    container: "Your Container"
    schema: exp
    table: data
    columns: [Name, Created, Run, Path_To_Synced_Data]
    filters:
      - { field: Path_To_Synced_Data, type: contains, value: "/scRNA/raw/" }

  - name: sample_manifest
    type: excel
    path: manifests/sample_manifest.xlsx    # relative to the drop folder unless absolute
    sheet: Sheet1                           # optional; defaults to active sheet

  - name: barcodes
    type: csv
    path: manifests/barcodes.csv            # relative to the drop folder unless absolute
    delimiter: ","                          # optional; default is comma

metadata_match:
  # Default key template used to render a key per file from regex variables
  key_template: <prefix>
  # Ordered rules (first match wins)
  search:
    - source: lk_experiments
      field: Uploaded_Filename_Prefix
      # key_template: <prefix>
    - source: sample_manifest
      field: Prefix
      key_template: <lib>_<seq>
```

Notes:
- `fields`: Mapping used to build LabKey rows during write-back. Supports placeholders and functions (`now()`, `drop_file_mtime()`).
- `field_parameters`:
  - `file_list`: identifies the field used for presence CONTAINS-check by target path (e.g., `Path To Synced Data`).
  - `file_list_aggregator`: reserved for future aggregation behavior during write-back.
- `replacements`:
  - `before_match` applies to captured/derived variables before templating and metadata matching.
  - `before_writeback` can transform variables (target: var) prior to rendering or fields (target: field) after rendering.

Example template `dm/sync.yml.TEMPLATE` (excerpt):

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

Typical usage (configuration is taken from `<drop_folder>/sync.yml`):

```bash
python dm/dm.py sync --drop-folder /path/to/drop
# Execute copy/write-back/move after confirmation
python dm/dm.py sync --drop-folder /path/to/drop --do-it
```

Notes:
- The regex is enforced for planning; non-matching files are listed as skipped by regex in discovery output.
- `<run>` increments to avoid collisions within a run when `filename_sequence: run`. If `hash` is enabled, `<hash>` is a deterministic CRC32.
- Integrity policy:
  - If `.md5` exists: MD5 must match; otherwise the file is skipped.
  - If `.md5` is absent: a `.blake3` sidecar is computed and written before copy.
- With `--do-it`, after a verified copy, sidecars are copied to the repository and originals are moved under `processed_folder` (preserving structure).

---

## How It Works (Detailed Flow)

Inside `dm/dm.py` (`sync` command):

1. Configuration load: The tool reads `<drop_folder>/sync.yml` directly based on the `--drop-folder` argument.
2. File discovery: Uses `glob` with `drop_filename_filter` to find candidate files in the drop folder; prints matched and regex-skipped lists.
3. Regex validation and capture: Validates each file path against `drop_filename_regex`; extracts named groups (e.g., `phase`, `seq`, `lib`, `prefix`, `suffix`).
4. Derivations and replacements: Optionally derive variables from matched metadata per `metadata_derive`, then apply `replacements.before_match` to captured/derived variables.
5. Target filename rendering: Render `repository_filename` with variables and `<source.Field>` placeholders (from matched metadata). Resolve `<run>` collisions or set `<hash>` per `filename_sequence`.
6. Integrity check:
   - If `.md5` exists: compute and compare MD5; mismatches skip copying.
   - If no `.md5`: compute BLAKE3 and write a `.blake3` sidecar prior to copy.
7. LabKey presence check: By default uses `field_parameters.file_list` with a CONTAINS filter on target path, or `presence_check.field` with `equal|contains`. Annotates each file with `in_labkey` and `existing_row`.
8. Pre-run reporting:
   - Plan table with key annotations including `write_action: update|create|skip_create`.
   - Dry-run write-back tables: update diffs and (unless `skip_creates`) planned create fields.
   - Copy plan summary: `would_copy` or `would_skip:<reason>`.
   - Archive/move plan summary: `would_move` or `would_skip:<reason>` (mirrors copy gating).
9. Execute mode (`--do-it`):
   - Enforce gating: metadata required, MD5 pass if present, and write-back viability (skip if `skip_creates` blocks or missing `RowId` for updates).
   - Copy source → target, verify by block-by-block compare, then copy matching sidecar to the repository.
   - Build write-back rows from `fields`; update existing rows by `RowId` and insert new rows unless `writeback.skip_creates: true`.
   - Post-run reporting: copy summary, update diffs, and (unless `skip_creates`) planned create fields.
   - Archive/move originals: after verified copy, move original files under `processed_folder` preserving the drop-relative structure; print archive/move summary.

---

## Output and Tables

Primary tables printed via `rich`:

- **[Plan table]** One row per file with key annotations. `write_action` shows `update|create|skip_create`. Rows with `skip_create` are highlighted yellow; MD5 mismatch is red.
- **[Dry-run write-back]** Per-row update diffs and (unless `skip_creates`) planned create fields.
- **[Copy plan summary]** `would_copy` or `would_skip:<reason>`; skipped rows highlighted red.
- **[Archive/move plan summary]** `would_move` or `would_skip:<reason>`; skipped rows highlighted red.
- **[Copy summary (executed)]** `copied` or `skipped:<reason>` with red highlighting for skips.
- **[Archive/move summary (executed)]** `moved` or `skipped:<reason>` with red highlighting for skips.

---

## Known Limitations and Gaps

- **[Metadata sources]** The default pipeline performs inline LabKey lookups only. Excel/CSV helpers exist in `dm/metadata.py` but are not currently invoked.
- **[Aggregation]** `file_list_aggregator` is reserved for future aggregation behavior.
- **[Field resolution]** Write-back maps field captions/names best-effort; verify your `fields` keys against LabKey when in doubt.
- **[Move semantics]** Archive/move skips when destination exists; collision/retention policies can be extended if needed.
- **[Imports]** Imports are module-safe with fallbacks for script execution; running via `python -m dm.dm` should work.

---

## Roadmap / Next Steps

- Optional: Excel/CSV metadata matching in the default pipeline.
- Optional: Aggregation using `file_list_aggregator`.
- Robust retries for LabKey write-back and file operations.
- Unit tests and CI.

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

# View planned actions (dry run)
python dm/dm.py sync --drop-folder /path/to/drop
```

---

## Troubleshooting

- Regex mismatch → program exits with error: Ensure `drop_filename_regex` matches the discovered file paths.
- LabKey errors: Review host/container/schema/table and your credentials/permissions. The tool catches and prints errors like `ServerContextError`, `ServerNotFoundError`, `QueryNotFoundError`, and `RequestError`.
- No `.md5` sidecar: The tool will compute and write a `.blake3` sidecar before copying; this is not treated as a failure.
- Configuration source: Only `<drop_folder>/sync.yml` is read; there is no global merge.
- Running as a module: Imports are module-safe with a fallback for direct script execution.

---

## License

TBD
