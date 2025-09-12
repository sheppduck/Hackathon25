import re
import logging
import pandas as pd


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names: strip, lowercase, remove punctuation, collapse whitespace to underscores."""
    def _clean(name: str) -> str:
        name = str(name).strip().lower()
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"_+", "_", name)
        return name
    df = df.rename(columns=_clean)
    logging.info("Cleaned column names")
    return df


def infer_and_parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Find columns that look like dates and attempt to parse them in place."""
    date_cols = [c for c in df.columns if re.search(r"date|dt|time", c)]
    parsed = []
    for c in date_cols:
        try:
            df[c] = pd.to_datetime(df[c], errors="coerce", infer_datetime_format=True)
            parsed.append(c)
        except Exception:
            logging.debug("Could not parse column as date: %s", c)
    logging.info("Parsed date columns: %s", parsed)
    return df


def downcast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast wide numeric dtypes to smaller memory-friendly types where safe."""
    num_cols = df.select_dtypes(include=["int64", "float64"]).columns
    for c in num_cols:
        if pd.api.types.is_integer_dtype(df[c]):
            df[c] = pd.to_numeric(df[c], downcast="integer")
        else:
            df[c] = pd.to_numeric(df[c], downcast="float")
    logging.info("Downcasted numeric columns to smaller dtypes where possible")
    return df


def detect_amount_column(df: pd.DataFrame):
    """Return a best-guess column name for monetary/amount columns, or None."""
    candidates = [c for c in df.columns if re.search(r"amount|charge|cost|paid|total", c)]
    return candidates[0] if candidates else None


def detect_id_columns(df: pd.DataFrame):
    """Detect likely patient and provider identifier columns."""
    patient_cols = [c for c in df.columns if re.search(r"patient(_)?id|member(_)?id|pid|member", c)]
    provider_cols = [c for c in df.columns if re.search(r"provider(_)?id|provider", c)]
    return patient_cols, provider_cols
