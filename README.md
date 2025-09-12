# claims_prep — quickstart and reference

Lightweight utilities to preprocess healthcare-claims CSVs and ingest them into sqlite
files for downstream exploration and feature engineering with pandas.

Quickstart
----------

1) Run the interactive demo (creates a temporary DB by default):

    ```powershell
    python -m claims_prep.demo
    ```

2) Create one sqlite database per dataset subdirectory under `data/` and write them to
   `databases/` (recommended for multiple independent datasets):

    ```powershell
    python -m claims_prep --create-db --all-datasets --data-dir .\data --databases-dir .\databases
    ```

3) Create a single DB for a specific dataset directory:

    ```powershell
    python -m claims_prep --create-db --data-dir .\data\dataset1 --db-path .\databases\dataset1.db
    ```

Command-line reference (recommended)
-----------------------------------

Primary launcher: `python -m claims_prep [OPTIONS]` (from repository root). The most commonly
used options are:

- `--create-db` — ingest CSVs into one sqlite DB and exit (use with `--all-datasets` or supply `--data-dir`).
- `--all-datasets` — when used with `--create-db`, create one DB per immediate subdirectory of `--data-dir` and write them to `--databases-dir`.
- `--data-dir` PATH (default: `data`) — data root or dataset directory to ingest.
- `--databases-dir` PATH (default: `databases`) — when using `--all-datasets`, destination folder for per-dataset DBs.
- `--db-path` PATH — DB path when creating a single DB. If omitted the CLI will derive a sensible default of `databases/<dataset_name>.db` based on `--data-dir`.
- `--no-preprocess` — skip cleaning/typing (column normalization, date parsing, numeric downcast) when ingesting CSVs.

Other useful options (single-CSV processing / interactive checks):

- `--input` / `-i` PATH — path to a single CSV to process (required unless `--create-db` is used).
- `--output` / `-o` PATH — output path for cleaned CSV (default: `processed_claims.csv`).
- `--nrows` INT — read only first N rows (useful for quick tests).
- `--hash-ids` / `--id-salt` — de-identify detected ID columns with deterministic hashing.
- `--compute-features` / `--features-output` — run lightweight feature engineering and save features CSV.

Demo details
------------

- `claims_prep.demo.demo_create_and_preview()` builds a sqlite DB from `data/` and previews
  a chosen table (`claims` by default). When `db_path` is omitted a temporary file is created
  and its path is logged so you can inspect or persist it.

You can run the demo using the committed small sample dataset:

```powershell
python -m claims_prep.demo --use-sample
```

This uses the `data/sample_small` CSVs committed for examples and tests.

Programmatic API (quick reference)
---------------------------------

- `create_sqlite_db_from_dir(data_dir: Path, db_path: Path, csv_glob: str = "*.csv", chunk_size: int = 100_000, preprocess: bool = True, if_exists: str = "replace")`
  - Ingest each CSV in `data_dir` into a table named after the file stem. Streams files in chunks to limit memory usage.
- `create_sqlite_databases_for_data_root(data_root: Path, databases_dir: Path, csv_glob: str = "*.csv", chunk_size: int = 100_000, preprocess: bool = True, if_exists: str = "replace")`
  - Create one sqlite DB per dataset directory and write into `databases_dir`.
- `list_db_tables(db_path: Path) -> List[str]` — list tables in a sqlite file.
- `read_table(db_path: Path, table: str, sql: Optional[str] = None) -> pandas.DataFrame` — read a table or query into pandas.

Notes, caveats, and next steps
------------------------------

- Datasets may have different CSV filenames and column sets. The ingestion treats each
  dataset independently and does not automatically align schemas across datasets.
  For cross-dataset modeling, add a schema-normalization mapping step (rename, cast, fill)
  before ingestion or request a helper to do conservative alignment during ingestion.
- The default preprocessing (clean names, parse dates, downcast numeric types) is safe
  for most exploratory workflows; use `--no-preprocess` if you need raw ingestion.
- The demo writes to a temporary DB by default to avoid overwriting local files; pass
  `db_path` if you need a persistent DB.
