"""
conftest.py — pytest configuration and shared fixtures.

Inserts the project root (snp/) into sys.path so all absolute imports resolve.
Stubs out optional third-party packages (yfinance, requests, bs4) so the suite
runs in offline / CI environments without those packages installed.
"""

import os
import sys
from unittest.mock import MagicMock

# ── Make the project root importable ─────────────────────────────────────────
# tests/ lives one level below the project root (snp/), so go up one level.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ── Stub optional dependencies ────────────────────────────────────────────────
for _pkg in ("yfinance", "requests", "bs4"):
    if _pkg not in sys.modules:
        try:
            __import__(_pkg)
        except ModuleNotFoundError:
            sys.modules[_pkg] = MagicMock()

# ── Shared helpers and fixtures ───────────────────────────────────────────────
import pandas as pd
import pytest


def make_ohlcv(dates: list[str], symbol: str = "AAPL") -> pd.DataFrame:
    """Return a minimal OHLCV DataFrame indexed by the given date strings."""
    idx = pd.to_datetime(dates)
    n   = len(dates)
    df  = pd.DataFrame(
        {
            "Open":   [100.0 + i for i in range(n)],
            "High":   [105.0 + i for i in range(n)],
            "Low":    [ 95.0 + i for i in range(n)],
            "Close":  [102.0 + i for i in range(n)],
            "Volume": [1_000_000 + i * 1_000 for i in range(n)],
        },
        index=idx,
    )
    df.index.name = "Date"
    return df


@pytest.fixture()
def tmp_data_dir(tmp_path):
    d = tmp_path / "eod_data"
    d.mkdir()
    return str(d)


@pytest.fixture()
def tmp_ami_dir(tmp_path):
    d = tmp_path / "amibroker_data"
    d.mkdir()
    return str(d)


@pytest.fixture()
def sample_df():
    return make_ohlcv(
        ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"]
    )
