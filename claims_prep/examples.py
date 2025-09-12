import logging
from typing import Optional

import pandas as pd

from .db import create_sqlite_db_from_dir, list_db_tables, read_table
from .io import preview_df


def summarize_claims(df: pd.DataFrame, amount_col: Optional[str] = None, group_by: Optional[str] = None) -> pd.DataFrame:
    """A small summary helper that returns aggregated metrics useful for quick inspection.

    This is intentionally light-weight and safe for interactive use.
    """
    amount_col = amount_col or next((c for c in df.columns if "amount" in c), None)
    gb = group_by or None
    if gb and gb in df.columns:
        agg = df.groupby(gb)[amount_col].agg(count="count", total="sum", mean="mean").reset_index()
    elif amount_col:
        agg = df[amount_col].agg(count="count", total="sum", mean="mean").to_frame().T
    else:
        agg = pd.DataFrame()
    logging.info("Summarized claims; rows: %d", len(agg))
    return agg


def example_filters(df: pd.DataFrame, amount_col: Optional[str] = None) -> pd.DataFrame:
    """Return a small example filtered DataFrame for demonstration/testing.

    This function demonstrates how a user might select suspicious rows.
    """
    amount_col = amount_col or next((c for c in df.columns if "amount" in c), None)
    if amount_col and amount_col in df.columns:
        return df[df[amount_col] > df[amount_col].quantile(0.99)]
    return df.head(0)
