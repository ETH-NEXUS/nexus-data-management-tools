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

The tool checks LabKey for existing rows that reference the planned target paths. In the current version, building rows and inserting/updating in LabKey are not implemented yet (see Roadmap). Moving files into a processed area is also planned but not yet implemented.

---

## Features

- Discovery of input files via glob pattern (e.g., `**/*.fastq.gz`) and a strict regex with named capture groups.
- Construction of target repository path using placeholders like `<phase>`, `<lib>`, `<seq>`, `<run>`, `<hash>`.
- Filename de-duplication via configurable sequence number (run) or deterministic short hash.
- MD5 verification when a `.md5` sidecar is present; if absent, a BLAKE3 digest is computed and a `.blake3` sidecar is written before copying.
- LabKey presence check for the target path using the `labkey` Python SDK.
- Tabular console output using `rich`, with colored styling for quick status review.
- Metadata-aware templating for repository filenames using `<source.Field>` placeholders after metadata matching.


---

## Architecture and Key Files

- `dm/dm.py`
  - `sync`: main command that performs discovery, validation, path rendering, checksum verification, LabKey query, and optional copying.
  - `check`: simple command to verify LabKey connectivity.
- `dm/config.py`
  - `options_from_source()`: `click` decorator that loads configuration from the `<drop_folder>/sync.yml` only (no global merge). Use `dm/sync.yml.TEMPLATE` as a starting point for creating per-drop configs.
- `dm/functions.py`
  - `now(date_format)`: returns current local time string in given format.
  - `drop_file_mtime(filename, date_format)`: returns mtime of a file.
- `dm/helpers.py`
  - `Message`: colored console messages.
  - `TableOutput`: pretty table printer using `rich`.
  - `Hasher`: `crc32`, `md5`, `blake3`, and block-by-block `equals` helpers (streaming, memory efficient).
- `dm/integrity.py`
  - Utilities to read `.md5` sidecars, compute/write `.blake3` sidecars, and copy a matching sidecar alongside the synced file.
- `dm/metadata.py`
  - Loaders for external metadata sources (LabKey, Excel, CSV) and support for matching metadata rows per file using configured rules.
- `dm/sync.yml.TEMPLATE`
  - Template configuration to copy into each drop folder as `<drop_folder>/sync.yml`.
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
- `processed_folder`: Root where processed (or archived) drop files should be moved. (Not yet implemented)
- `filename_sequence`: Either `run` (increments `<run>`) or `hash` (sets `<hash>` to a short CRC32-derived value).
- `date_format`: Datetime format string intended for use by helper functions like `now()` or `drop_file_mtime()`. Reserved for future LabKey write-back (not used by the core flow yet).
- `labkey`:
  - `host`: LabKey host
  - `container`: LabKey container/folder (e.g., `LOOP Intercept`)
  - `schema`: Target schema (e.g., `exp`) — see Known Limitations for a note
  - `table`: Target table (e.g., `data` or `scRNA_Experiments`) — see Known Limitations
  - `context`: Optional; not currently used.
- `metadata_sources`: A list of external metadata sources to load during `sync`. Each item has a `type` and
  type-specific fields. Supported types:
  - `labkey`: Load rows from a LabKey table using the configured host/container/schema/table.
  - `excel`: Load rows from an Excel file (`.xlsx`) using `openpyxl`.
  - `csv`: Load rows from a CSV file using Python's csv module.
- `metadata_match`: Rules to find the metadata row for each file before syncing.
  - `key_template`: Default template string to render a metadata key from the filename regex variables (e.g., `<prefix>`, `<lib>_<seq>`).
  - `search`: Ordered list of rule objects, each with:
    - `source`: Name of the source from `metadata_sources` to search.
    - `field`: Field/column name within that source to match against.
    - `key_template` (optional): Override the default template for this rule.
- `metadata_required`: Boolean flag. If `true`, files without a matching metadata row are skipped (reason `metadata_missing`).

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
- Excel support requires `openpyxl`. This repository includes it in `environment.yml`.
- `fields`: Reserved for future LabKey insert/upsert. In the current version, no rows are built or written, so this mapping is not processed yet.
- `field_parameters`:
  - `file_list`: the field to search with a `CONTAINS` filter when checking LabKey for existing rows (e.g., `Path_To_Synced_Data`).
  - `file_list_aggregator`: reserved for future aggregation behavior during write-back.
- `lookups`: Reserved for future value translation, e.g., mapping `phase: btki -> BTKi`. Not used by the current flow.

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
  - If `.md5` sidecar is absent: a `.blake3` sidecar is created for the source before copying (and later copied alongside the target).

### check

Validate LabKey connectivity and basic query capability:

```bash
python dm/dm.py check -h your-labkey-host.example.org -c "Your Container" -s exp -t data
```

The command prints the result of a simple `select_rows`.

---

## How It Works (Detailed Flow)

Inside `dm/dm.py` (`sync` command):

1. Configuration load: Using `options_from_source("--drop-folder")` from `dm/config.py`, the tool reads `<drop_folder>/sync.yml` only. No global merge is performed.
2. File discovery: Uses `glob` with `drop_filename_filter` to find candidate files in the drop folder.
3. Regex validation and capture: Validates each file path against `drop_filename_regex`; extracts named groups (e.g., `phase`, `seq`, `lib`, `prefix`, `suffix`).
4. Target filename rendering: Renders `repository_filename` by replacing placeholders. Resolves `<run>` collision or sets `<hash>` depending on `filename_sequence`.
5. LabKey row building: Not implemented in the current version. The `fields`, `field_parameters`, and `lookups` sections are reserved for future write-back.
6. Aggregation: Not performed yet. If/when write-back is implemented, `file_list_aggregator` will drive grouping.
7. Pre-copy integrity:
   - If a sidecar `.md5` exists: compute the file's MD5 (streamed) and compare to the sidecar value. Files with a mismatch are flagged and will be skipped during copy.
   - If no sidecar `.md5` exists: the tool will compute a BLAKE3 digest and write a `.blake3` sidecar for the source file before copying.
8. Optional metadata loading: If `metadata_sources` is configured, each source is loaded (LabKey/Excel/CSV) and a summary table (name, type, count, status) is printed.
9. Optional per-file metadata matching: If `metadata_match` is configured, the tool renders a key per file using the filename regex variables and searches the configured source/field for a matching row. Each file is annotated with `meta_found`, `meta_source`, and `meta_key`.
10. LabKey presence check: Queries LabKey (`QueryFilter` with `CONTAINS` on your configured `file_list_field`) to set `in_labkey=True/False` for each planned target file.
11. Reporting: Prints a table of planned sync actions with color cues.
12. Copy (if `--do-it`):
    - If `.md5` sidecar exists and matches: proceed to copy.
    - If `.md5` sidecar exists and mismatches: skip copy and report an error.
    - If no `.md5` sidecar exists: compute and write a `.blake3` sidecar for the source file, then copy.
    - After copy: verify by block-by-block compare. If verification succeeds, copy the matching sidecar (`.md5` or `.blake3`) to the repository alongside the main file. Then print a second table showing copy status.
    - If `metadata_required: true` and a file has no matching metadata row, it is skipped with `reason: metadata_missing`.

---

## Output and Tables

Two tables are printed using `rich`:

- Pre-copy table (planning and integrity status):
  - `source`
  - `target`
  - `integrity_method`: md5 or blake3
  - `md5`: computed MD5 (if applicable)
  - `orig_md5`: value from `.md5` sidecar (if present)
  - `md5_ok`: result of the MD5 match (if applicable)
  - `meta_found`: whether a metadata row was found according to `metadata_match`
  - `meta_source`: which source produced the match
  - `meta_key`: the rendered key used for matching
  - `in_labkey`: Whether LabKey already has a row referencing the target path

- Post-copy table (copy and verification status):
  - `source`
  - `target`
  - `copy_ok`: Whether the copy operation created the target file
  - `verified`: Result of block-by-block comparison between source and target
  - `sidecar`: Which sidecar was used/copied (`md5`, `blake3`, or empty if none)
  - `sidecar_copy_ok`: Whether sidecar copy to the target location succeeded

Color highlighting:
- Pre-copy: Red rows indicate a `.md5` sidecar exists and the computed MD5 does not match (`md5_ok == False`). Missing `.md5` sidecars are handled by BLAKE3 and are not considered failures.
- Pre-copy: Yellow rows indicate metadata matching did not find a row (`meta_found == False`), if `metadata_match` is configured.
- Post-copy: Yellow rows indicate `copy_ok == False`; Red rows indicate `verified == False` or a sidecar was expected/copied (`md5`/`blake3`) but `sidecar_copy_ok == False`.

---

## Known Limitations and Gaps

- LabKey insert/update not implemented: No rows are prepared or written yet.
- Move to processed not implemented: `processed_folder` is defined but not used yet to move files after a successful sync.
- Aggregation not implemented: `file_list_aggregator` is reserved for future use during write-back.
- Integrity policy: Missing `.md5` is handled by BLAKE3 sidecars and is not flagged as a failure.
- LabKey schema/table pairing: Ensure `schema` (e.g., `exp`) and `table` (e.g., `data`, or your custom table) are configured as separate values. The example template shows a consistent pair.
- Unused `labkey.context`: Present in config but not passed to `APIWrapper`.
- Type handling: Because no rows are written, type coercion is not applicable yet. When write-back is added, preserve booleans/numbers instead of casting to strings.
- Import style: Imports are module-safe with a fallback for script execution; running via `python -m dm.dm` should work.

---

## Roadmap / Next Steps

- Implement LabKey `insert_rows`/`upsert_rows` with retries and clear error handling.
- Decide whether to insert per-file rows or aggregated rows keyed by `file_list_aggregator`.
- Implement moving successfully processed drop files into `processed_folder`, preserving structure.
- Implement row building from `fields` with proper type preservation and optional `lookups` translation.
- Add unit tests for config load, metadata matching, path rendering, and copy/verify flows.
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
- No `.md5` sidecar: The tool will compute and write a `.blake3` sidecar before copying; this is not treated as a failure.
- Configuration source: Only `<drop_folder>/sync.yml` is read; there is no global merge.
- Running as a module: Imports are module-safe with a fallback for direct script execution.

---

## License

TBD
