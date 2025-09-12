import logging
import tempfile
from pathlib import Path
from typing import Optional

from .db import create_sqlite_db_from_dir, list_db_tables, read_table
from .io import preview_df


def demo_create_and_preview(data_dir: Path = Path("data"), db_path: Optional[Path] = None,
                            table_to_preview: str = "claims", n_preview: int = 5) -> None:
    """Small demo: build a DB from CSVs, list tables, and show a preview of a chosen table.

    When `db_path` is not supplied a temporary file is created so the demo does not
    overwrite any local artifacts (useful for CI and exploratory runs).
    """
    # use a temporary file when db_path not provided to avoid accidental overwrites
    if db_path is None:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        db_path = Path(tmp.name)
        tmp.close()
        logging.info("Demo: using temporary sqlite DB at %s", db_path)

    logging.info("Demo: creating sqlite DB from %s -> %s", data_dir, db_path)
    create_sqlite_db_from_dir(data_dir, db_path)
    try:
        tables = list_db_tables(db_path)
        logging.info("Tables in DB: %s", tables)
    except Exception:
        logging.exception("Failed to list tables in the DB")
        return

    if table_to_preview in tables:
        try:
            df = read_table(db_path, table_to_preview)
            logging.info("Previewing table %s (first %d rows)", table_to_preview, n_preview)
            preview_df(df, n=n_preview)
        except Exception:
            logging.exception("Failed to read or preview table %s", table_to_preview)
    else:
        logging.warning("Table %s not present in DB; available: %s", table_to_preview, tables)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    p = argparse.ArgumentParser(description="Run the demo that ingests sample data and previews a table.")
    p.add_argument("--use-sample", action="store_true", help="Use the committed sample dataset (data/sample_small)")
    p.add_argument("--data-dir", type=Path, default=Path("data"), help="Data directory to ingest")
    p.add_argument("--db-path", type=Path, default=None, help="If set, persist the demo DB to this path")
    args = p.parse_args()

    data_dir = Path("data/sample_small") if args.use_sample else args.data_dir
    demo_create_and_preview(data_dir=data_dir, db_path=args.db_path)
