from pathlib import Path
import logging
import pandas as pd


def load_csv(path: Path, nrows: int = None, low_memory: bool = False, **read_kwargs) -> pd.DataFrame:
    """Load a CSV into a pandas DataFrame with logging and safe error handling."""
    logging.info("Loading CSV: %s", path)
    try:
        df = pd.read_csv(path, nrows=nrows, low_memory=low_memory, **read_kwargs)
    except Exception as e:
        logging.error("Failed to read CSV: %s", e)
        raise
    logging.info("Loaded %d rows x %d columns", df.shape[0], df.shape[1])
    return df


def save_csv(df: pd.DataFrame, out_path: Path):
    """Save a DataFrame to CSV, creating parent dirs as needed."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logging.info("Saved processed CSV to %s", out_path)


def preview_df(df: pd.DataFrame, n: int = 5) -> None:
    """Print a short preview of a DataFrame for interactive use."""
    print("\n== SHAPE ==\n", df.shape)
    print("\n== DTYPE SAMPLE ==\n", df.dtypes.head(20))
    print(f"\n== HEAD ({n}) ==\n", df.head(n))
    print(f"\n== TAIL ({n}) ==\n", df.tail(n))
    print("\n== MISSING COUNTS (top 20) ==\n", df.isna().sum().sort_values(ascending=False).head(20))
