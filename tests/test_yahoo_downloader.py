"""Unit tests for downloader.yahoo.YahooDownloader — no network calls."""

from unittest.mock import patch

import pandas as pd
import pytest

from tests.conftest import make_ohlcv
from downloader.yahoo import YahooDownloader


def _yf_df(dates: list[str]) -> pd.DataFrame:
    """Simulate yf.download output: tz-aware UTC index."""
    df = make_ohlcv(dates)
    df.index = pd.to_datetime(df.index).tz_localize("UTC")
    return df


class TestYahooDownloaderSuccess:
    def test_returns_dataframe_on_success(self):
        with patch("downloader.yahoo.yf.download", return_value=_yf_df(["2024-01-02", "2024-01-03"])):
            result = YahooDownloader(retries=1, retry_delay=0).fetch("AAPL", "2024-01-02", "2024-01-04")
        assert result is not None and len(result) == 2

    def test_columns_are_ohlcv(self):
        with patch("downloader.yahoo.yf.download", return_value=_yf_df(["2024-01-02"])):
            result = YahooDownloader(retries=1, retry_delay=0).fetch("AAPL", "2024-01-02", "2024-01-03")
        assert list(result.columns) == ["Open", "High", "Low", "Close", "Volume"]

    def test_index_name_is_date(self):
        with patch("downloader.yahoo.yf.download", return_value=_yf_df(["2024-01-02"])):
            result = YahooDownloader(retries=1, retry_delay=0).fetch("AAPL", "2024-01-02", "2024-01-03")
        assert result.index.name == "Date"

    def test_index_is_tz_naive(self):
        with patch("downloader.yahoo.yf.download", return_value=_yf_df(["2024-01-02"])):
            result = YahooDownloader(retries=1, retry_delay=0).fetch("AAPL", "2024-01-02", "2024-01-03")
        assert result.index.tz is None

    def test_values_rounded_to_4dp(self):
        df = _yf_df(["2024-01-02"])
        df["Close"] = 123.456789
        with patch("downloader.yahoo.yf.download", return_value=df):
            result = YahooDownloader(retries=1, retry_delay=0).fetch("AAPL", "2024-01-02", "2024-01-03")
        assert result["Close"].iloc[0] == pytest.approx(123.4568)

    def test_flattens_multiindex_columns(self):
        df = _yf_df(["2024-01-02"])
        df.columns = pd.MultiIndex.from_tuples([(c, "AAPL") for c in df.columns])
        with patch("downloader.yahoo.yf.download", return_value=df):
            result = YahooDownloader(retries=1, retry_delay=0).fetch("AAPL", "2024-01-02", "2024-01-03")
        assert not isinstance(result.columns, pd.MultiIndex)


class TestYahooDownloaderEmpty:
    def test_returns_none_on_empty_dataframe(self):
        with patch("downloader.yahoo.yf.download", return_value=pd.DataFrame()):
            result = YahooDownloader(retries=1, retry_delay=0).fetch("DELISTED", "2024-01-01", "2024-01-02")
        assert result is None


class TestYahooDownloaderRetry:
    def test_retries_on_exception_then_succeeds(self):
        calls = {"n": 0}
        good  = _yf_df(["2024-01-02"])

        def flaky(*a, **kw):
            calls["n"] += 1
            if calls["n"] < 2:
                raise ConnectionError("transient")
            return good

        with patch("downloader.yahoo.yf.download", side_effect=flaky):
            with patch("downloader.yahoo.time.sleep"):
                result = YahooDownloader(retries=3, retry_delay=0).fetch("AAPL", "2024-01-02", "2024-01-03")

        assert result is not None
        assert calls["n"] == 2

    def test_returns_none_after_all_retries_exhausted(self):
        with patch("downloader.yahoo.yf.download", side_effect=ConnectionError("down")):
            with patch("downloader.yahoo.time.sleep"):
                result = YahooDownloader(retries=3, retry_delay=0).fetch("AAPL", "2024-01-01", "2024-01-02")
        assert result is None

    def test_exact_retry_count(self):
        with patch("downloader.yahoo.yf.download", side_effect=RuntimeError("err")) as mock_dl:
            with patch("downloader.yahoo.time.sleep"):
                YahooDownloader(retries=3, retry_delay=0).fetch("AAPL", "2024-01-01", "2024-01-02")
        assert mock_dl.call_count == 3
