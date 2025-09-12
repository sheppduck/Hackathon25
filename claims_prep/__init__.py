"""Package entry for lightweight claims preprocessing helpers.

Expose a compact public API so callers can do:

    from claims_prep import load_csv, create_fraud_features

Keep imports minimal here to avoid heavy startup cost or side effects.
"""

from .io import load_csv, save_csv, preview_df
from .cleaning import (
    clean_column_names,
    infer_and_parse_dates,
    downcast_numeric,
    detect_amount_column,
    detect_id_columns,
)
from .features import create_fraud_features, deidentify_ids
from .examples import summarize_claims, example_filters
from .db import create_sqlite_db_from_dir, read_table, list_db_tables, csv_to_table
from .demo import demo_create_and_preview
from .db import create_sqlite_databases_for_data_root

__all__ = [
    "load_csv",
    "save_csv",
    "preview_df",
    "clean_column_names",
    "infer_and_parse_dates",
    "downcast_numeric",
    "detect_amount_column",
    "detect_id_columns",
    "create_fraud_features",
    "deidentify_ids",
    "summarize_claims",
    "example_filters",
    "create_sqlite_db_from_dir",
    "read_table",
    "list_db_tables",
    "csv_to_table",
    "demo_create_and_preview",
    "create_sqlite_databases_for_data_root",
]
