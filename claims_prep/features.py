import hashlib
import logging
from typing import List, Optional

import numpy as np
import pandas as pd

from .cleaning import detect_id_columns, detect_amount_column


def deidentify_ids(df: pd.DataFrame, id_cols: List[str], salt: str = "") -> pd.DataFrame:
    """
    Replace sensitive ID columns with deterministic hashes.
    This keeps linkability without exposing raw identifiers.
    By default the raw column is dropped and a suffix "_hash" is added.
    """
    if not id_cols:
        return df
    for c in id_cols:
        new_col = f"{c}_hash"
        logging.info("Hashing id column %s -> %s", c, new_col)
        df[new_col] = df[c].astype(str).fillna("").apply(
            lambda x: hashlib.blake2b((x + salt).encode("utf-8"), digest_size=10).hexdigest()
        )
        # Drop raw column to avoid saving PHI
        if c != new_col:
            df = df.drop(columns=[c])
    return df


def create_fraud_features(df: pd.DataFrame, amount_col: Optional[str] = None, date_col: Optional[str] = None) -> pd.DataFrame:
    """
    Create lightweight features useful for downstream fraud/anomaly detection training.

    This function delegates internal steps to private helpers; it returns an enriched copy
    of the input DataFrame and does not fit any model.
    """
    df = df.copy()

    # amount-based features
    amount_col = amount_col or detect_amount_column(df)
    if amount_col:
        df["amount"] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)
        df["amount_log1p"] = np.log1p(df["amount"].clip(lower=0))
        df["amount_z"] = (df["amount"] - df["amount"].mean()) / (df["amount"].std() + 1e-9)

    # date features
    if date_col is None:
        date_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
        date_col = date_cols[0] if date_cols else None

    if date_col:
        df["_claim_dt"] = pd.to_datetime(df[date_col], errors="coerce")
        df["claim_dayofweek"] = df["_claim_dt"].dt.dayofweek.fillna(-1).astype(int)
        df["claim_hour"] = df["_claim_dt"].dt.hour.fillna(-1).astype(int)
    else:
        df["_claim_dt"] = pd.NaT

    # detect IDs
    patient_cols, provider_cols = detect_id_columns(df)
    patient_col = patient_cols[0] if patient_cols else None
    provider_col = provider_cols[0] if provider_cols else None

    # per-patient aggregations
    if patient_col and "amount" in df.columns:
        agg = df.groupby(patient_col)["amount"].agg(
            patient_claim_count="count",
            patient_total_amount="sum",
            patient_mean_amount="mean",
            patient_std_amount="std",
        )
        df = df.merge(agg, how="left", left_on=patient_col, right_index=True)
        if date_col:
            df = df.sort_values([patient_col, "_claim_dt"])
            df["prev_dt"] = df.groupby(patient_col)["_claim_dt"].shift(1)
            df["days_since_prev_claim"] = (df["_claim_dt"] - df["prev_dt"]).dt.days.fillna(-1)
            df = df.sort_index()

    # per-provider aggregations
    if provider_col and "amount" in df.columns:
        agg_p = df.groupby(provider_col)["amount"].agg(
            provider_claim_count="count",
            provider_total_amount="sum",
            provider_mean_amount="mean",
        )
        df = df.merge(agg_p, how="left", left_on=provider_col, right_index=True)

    # count unique diagnosis/procedure codes if such columns exist
    code_cols = [c for c in df.columns if pd.api.types.is_string_dtype(df[c]) and any(substr in c for substr in ("dx", "diagnosis", "cpt", "procedure", "hcpcs"))]
    if code_cols and patient_col:
        uniq_codes = df.groupby(patient_col)[code_cols].nunique().sum(axis=1).rename("patient_unique_codes")
        df = df.merge(uniq_codes, how="left", left_on=patient_col, right_index=True)

    # fill NaNs and ensure numeric feature columns exist
    numeric_features = [
        "amount", "amount_log1p", "amount_z",
        "patient_claim_count", "patient_total_amount", "patient_mean_amount", "patient_std_amount",
        "provider_claim_count", "provider_total_amount", "provider_mean_amount",
        "days_since_prev_claim", "patient_unique_codes"
    ]
    for f in numeric_features:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors="coerce").fillna(0.0)

    logging.info("Created fraud-focused features; sample columns: %s", [c for c in df.columns if c in numeric_features])
    return df
