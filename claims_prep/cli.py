"""Command-line interface wiring for the claims_prep package."""
from pathlib import Path
import argparse
import logging

from .io import load_csv, save_csv, preview_df
from .cleaning import clean_column_names, infer_and_parse_dates, downcast_numeric, detect_amount_column, detect_id_columns
from .features import create_fraud_features, deidentify_ids
from .examples import summarize_claims, example_filters
from .db import create_sqlite_db_from_dir, list_db_tables, read_table, create_sqlite_databases_for_data_root


def _configure_logging(level: int = logging.INFO):
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def main(argv: list = None) -> None:
    _configure_logging()
    p = argparse.ArgumentParser(description="Prepare healthcare claims CSV for downstream modeling (no ML model fitting).")
    p.add_argument("--input", "-i", type=Path, required=False, help="Path to input CSV")
    p.add_argument("--output", "-o", type=Path, default=Path("processed_claims.csv"), help="Where to save cleaned CSV")
    p.add_argument("--nrows", type=int, default=None, help="If set, read only nrows (useful for quick tests)")
    p.add_argument("--hash-ids", action="store_true", help="Hash detected ID columns to de-identify")
    p.add_argument("--id-salt", type=str, default="", help="Optional salt for deterministic hashing")
    p.add_argument("--compute-features", action="store_true", help="Create features useful for modeling (no model fitting)")
    p.add_argument("--features-output", type=Path, default=Path("claims_with_features.csv"), help="Where to save CSV with engineered features")
    p.add_argument("--create-db", action="store_true", help="Create a sqlite DB from CSVs in a data dir and exit")
    p.add_argument("--data-dir", type=Path, default=Path("data"), help="Directory containing CSV files to ingest into sqlite")
    p.add_argument("--db-path", type=Path, default=None, help="Path for sqlite DB to create/use. If omitted when creating a single dataset DB, the path will be derived under --databases-dir")
    p.add_argument("--no-preprocess", action="store_true", help="Skip cleaning/typing while ingesting CSVs into sqlite")
    p.add_argument("--all-datasets", action="store_true", help="When used with --create-db: create one sqlite DB per dataset subdirectory under --data-dir and write them to --databases-dir")
    p.add_argument("--databases-dir", type=Path, default=Path("databases"), help="Directory to write per-dataset sqlite files when using --all-datasets")

    args = p.parse_args(argv)

    # allow the --create-db flow to run without --input; require input for the normal processing path
    if not args.create_db and args.input is None:
        p.error("--input is required when not creating a DB")

    # If user asked to create a DB, do that and exit early
    if args.create_db:
        try:
            if args.all_datasets:
                created = create_sqlite_databases_for_data_root(args.data_dir, args.databases_dir, preprocess=not args.no_preprocess)
                logging.info("Created databases: %s", created)
            else:
                # Derive a sensible default db-path when none was provided: use databases/<dataset_name>.db
                if args.db_path is None:
                    dataset_name = Path(args.data_dir).name
                    db_path = args.databases_dir / f"{dataset_name}.db"
                else:
                    db_path = args.db_path

                create_sqlite_db_from_dir(args.data_dir, db_path, preprocess=not args.no_preprocess)
                logging.info("Created sqlite DB at %s", db_path)
                try:
                    tables = list_db_tables(db_path)
                    logging.info("DB tables: %s", tables)
                except Exception:
                    logging.debug("Could not list tables after creation")
        except Exception as e:
            logging.error("Failed to create sqlite DB: %s", e)
        return

    df = load_csv(args.input, nrows=args.nrows, low_memory=False)
    df = clean_column_names(df)
    df = infer_and_parse_dates(df)
    df = downcast_numeric(df)

    # Optional de-identification: hash id columns (patient/provider)
    if args.hash_ids:
        patient_cols, provider_cols = detect_id_columns(df)
        ids_to_hash = patient_cols + [c for c in provider_cols if c not in patient_cols]
        if not ids_to_hash:
            logging.warning("No ID-like columns detected to hash")
        else:
            df = deidentify_ids(df, ids_to_hash, salt=args.id_salt)

    preview_df(df, n=5)

    amount_col = detect_amount_column(df)

    # Optional lightweight summaries and filters
    try:
        summarize = summarize_claims(df, amount_col=amount_col, group_by="provider" if "provider" in df.columns else None)
        logging.info("Example summary:\n%s", summarize.head().to_string())
    except Exception:
        logging.debug("summarize_claims helper failed; skipping summary.")

    try:
        filters = example_filters(df, amount_col=amount_col)
        logging.info("Example filters: %d rows flagged", len(filters))
    except Exception:
        logging.debug("example_filters helper failed; skipping filters.")

    # Feature engineering only (no ML)
    if args.compute_features:
        try:
            df_feats = create_fraud_features(df, amount_col=amount_col)
            save_csv(df_feats, args.features_output)
            logging.info("Saved feature-engineered dataset to %s", args.features_output)
        except Exception as e:
            logging.error("Failed to compute features: %s", e)

    # save cleaned DataFrame
    save_csv(df, args.output)


if __name__ == "__main__":
    main()
