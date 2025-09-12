from pathlib import Path
import logging
import sqlite3
from typing import Iterable, List, Optional

import pandas as pd

from .cleaning import clean_column_names, infer_and_parse_dates, downcast_numeric
from .io import load_csv


def _connect(db_path: Path) -> sqlite3.Connection:
    """Return a sqlite3 connection; parents for file are created by caller if needed."""
    conn = sqlite3.connect(str(db_path))
    return conn


def create_sqlite_db_from_dir(data_dir: Path, db_path: Path, csv_glob: str = "*.csv", chunk_size: int = 100_000,
                              preprocess: bool = True, if_exists: str = "replace") -> None:
    """Create or update a sqlite database by ingesting all CSV files in `data_dir`.

    Each CSV becomes a table named after the CSV filename (stem). Files are read in
    streaming chunks to avoid large memory usage. When `preprocess` is True the
    helpers from `claims_prep.cleaning` are applied to each chunk before writing.

    Parameters
    - data_dir: Path containing CSV files
    - db_path: Path to sqlite file to create/modify
    - csv_glob: glob pattern for CSV files
    - chunk_size: rows per chunk for streaming read
    - preprocess: whether to run clean_column_names, infer_and_parse_dates, downcast_numeric
    - if_exists: behavior for existing tables: 'replace' or 'append'
    """
    data_dir = Path(data_dir)
    db_path = Path(db_path)
    files = sorted(data_dir.glob(csv_glob))
    if not files:
        logging.warning("No CSV files found in %s matching %s", data_dir, csv_glob)
        return

    # Ensure parent exists for db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(db_path)

    try:
        for f in files:
            table = f.stem
            logging.info("Ingesting %s -> table %s (chunksize=%d)", f, table, chunk_size)
            first_chunk = True
            for chunk in pd.read_csv(f, chunksize=chunk_size):
                if preprocess:
                    chunk = clean_column_names(chunk)
                    chunk = infer_and_parse_dates(chunk)
                    chunk = downcast_numeric(chunk)
                # pandas.to_sql with a sqlite3.Connection works; use replace on first chunk if requested
                mode = "replace" if first_chunk and if_exists == "replace" else "append"
                chunk.to_sql(table, conn, if_exists=mode, index=False)
                first_chunk = False
            logging.info("Finished ingesting %s -> %s", f, table)
    finally:
        conn.close()


def list_db_tables(db_path: Path) -> List[str]:
    """Return list of table names in the sqlite database."""
    conn = _connect(db_path)
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cur.fetchall()]
        return tables
    finally:
        conn.close()


def read_table(db_path: Path, table: str, sql: Optional[str] = None) -> pd.DataFrame:
    """Read an entire table (or an arbitrary SQL query) from sqlite into a pandas DataFrame.

    If `sql` is provided it is run instead of a simple SELECT * FROM table.
    """
    conn = _connect(db_path)
    try:
        if sql is None:
            sql = f"SELECT * FROM {table}"
        df = pd.read_sql_query(sql, conn)
    finally:
        conn.close()
    return df


def csv_to_table(csv_path: Path, db_path: Path, table: Optional[str] = None, preprocess: bool = True) -> None:
    """Helper to load a single CSV into sqlite (small files loaded wholly)."""
    csv_path = Path(csv_path)
    tname = table or csv_path.stem
    logging.info("Loading CSV %s into table %s", csv_path, tname)
    df = load_csv(csv_path)
    if preprocess:
        df = clean_column_names(df)
        df = infer_and_parse_dates(df)
        df = downcast_numeric(df)
    conn = _connect(db_path)
    try:
        df.to_sql(tname, conn, if_exists="replace", index=False)
    finally:
        conn.close()


def create_sqlite_databases_for_data_root(data_root: Path, databases_dir: Path, csv_glob: str = "*.csv",
                                         chunk_size: int = 100_000, preprocess: bool = True, if_exists: str = "replace") -> List[Path]:
    """Scan a root data directory for dataset subdirectories and create one sqlite DB
    per dataset in `databases_dir`.

    Each child directory of `data_root` that contains CSV files will produce a DB
    named `<databases_dir>/<dataset_name>.db`.

    Returns a list of created DB paths.
    """
    data_root = Path(data_root)
    databases_dir = Path(databases_dir)
    databases_dir.mkdir(parents=True, exist_ok=True)

    created: List[Path] = []
    for child in sorted(data_root.iterdir()):
        if not child.is_dir():
            continue
        files = list(child.glob(csv_glob))
        if not files:
            logging.info("Skipping %s: no CSV files found", child)
            continue
        db_path = databases_dir / f"{child.name}.db"
        logging.info("Creating DB for dataset %s -> %s", child.name, db_path)
        try:
            create_sqlite_db_from_dir(child, db_path, csv_glob=csv_glob, chunk_size=chunk_size, preprocess=preprocess, if_exists=if_exists)
            created.append(db_path)
        except Exception:
            logging.exception("Failed to create DB for dataset %s", child.name)
    logging.info("Created %d databases under %s", len(created), databases_dir)
    return created
