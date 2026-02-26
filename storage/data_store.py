import os
import re

import pandas as pd

from config import DATA_DIR

# Characters that are illegal in filenames on Windows: \ / : * ? " < > |
# The ^ caret used by Yahoo Finance index tickers (^GSPC) is legal on Windows
# but we normalise it to avoid surprises on other platforms.
_UNSAFE = re.compile(r'[\\/:*?"<>|^]')


def _safe_filename(symbol: str) -> str:
    """
    Convert a symbol to a safe filename stem.
    ^GSPC  -> _GSPC
    BRK-B  -> BRK-B   (unchanged)
    """
    return _UNSAFE.sub("_", symbol)


class DataStore:
    """
    Reads and writes per-symbol OHLCV CSV files.
    Each file is named <safe_symbol>.csv with columns: Date, Open, High, Low, Close, Volume.

    Index tickers that start with ^ are stored with _ prefix (_GSPC.csv) so the
    filename is safe on all platforms and the original symbol is preserved in the data.
    """

    def __init__(self, data_dir: str = DATA_DIR):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def csv_path(self, symbol: str) -> str:
        return os.path.join(self.data_dir, f"{_safe_filename(symbol)}.csv")

    def exists(self, symbol: str) -> bool:
        return os.path.exists(self.csv_path(symbol))

    def read(self, symbol: str) -> pd.DataFrame | None:
        try:
            return pd.read_csv(self.csv_path(symbol), index_col="Date", parse_dates=True)
        except Exception:
            return None

    def write(self, symbol: str, df: pd.DataFrame):
        df.to_csv(self.csv_path(symbol))

    def merge_and_write(self, symbol: str, new_df: pd.DataFrame) -> pd.DataFrame:
        """Append new rows to existing data, dedup, sort, and save."""
        existing = self.read(symbol)
        if existing is not None:
            combined = pd.concat([existing, new_df])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined.sort_index(inplace=True)
        else:
            combined = new_df
        self.write(symbol, combined)
        return combined
