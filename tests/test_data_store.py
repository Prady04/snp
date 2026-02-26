"""Unit tests for storage.data_store.DataStore"""

import pandas as pd
import pytest

from tests.conftest import make_ohlcv
from storage.data_store import DataStore


class TestDataStorePaths:
    def test_csv_path_format(self, tmp_data_dir):
        store = DataStore(tmp_data_dir)
        assert store.csv_path("AAPL").endswith("AAPL.csv")

    def test_exists_false_when_no_file(self, tmp_data_dir):
        store = DataStore(tmp_data_dir)
        assert store.exists("AAPL") is False

    def test_exists_true_after_write(self, tmp_data_dir, sample_df):
        store = DataStore(tmp_data_dir)
        store.write("AAPL", sample_df)
        assert store.exists("AAPL") is True


class TestDataStoreReadWrite:
    def test_write_then_read_roundtrip(self, tmp_data_dir, sample_df):
        store = DataStore(tmp_data_dir)
        store.write("AAPL", sample_df)
        result = store.read("AAPL")
        assert result is not None
        assert list(result.columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert len(result) == len(sample_df)
        assert result.index.name == "Date"

    def test_read_returns_none_for_missing_symbol(self, tmp_data_dir):
        assert DataStore(tmp_data_dir).read("MISSING") is None

    def test_read_returns_none_for_corrupt_file(self, tmp_data_dir):
        store = DataStore(tmp_data_dir)
        with open(store.csv_path("BAD"), "w") as f:
            f.write("not,valid,csv\n!!!\n")
        assert store.read("BAD") is None

    def test_index_is_datetime(self, tmp_data_dir, sample_df):
        store = DataStore(tmp_data_dir)
        store.write("AAPL", sample_df)
        result = store.read("AAPL")
        assert pd.api.types.is_datetime64_any_dtype(result.index)


class TestDataStoreMergeAndWrite:
    def test_merge_appends_new_rows(self, tmp_data_dir):
        store = DataStore(tmp_data_dir)
        store.write("AAPL", make_ohlcv(["2024-01-02", "2024-01-03"]))
        combined = store.merge_and_write("AAPL", make_ohlcv(["2024-01-04", "2024-01-05"]))
        assert len(combined) == 4

    def test_merge_deduplicates_overlapping_rows(self, tmp_data_dir):
        store = DataStore(tmp_data_dir)
        store.write("AAPL", make_ohlcv(["2024-01-02", "2024-01-03"]))
        combined = store.merge_and_write("AAPL", make_ohlcv(["2024-01-03", "2024-01-04"]))
        assert len(combined) == 3

    def test_merge_keeps_last_value_on_duplicate(self, tmp_data_dir):
        store = DataStore(tmp_data_dir)
        old = make_ohlcv(["2024-01-02"])
        old["Close"] = 999.0
        store.write("AAPL", old)
        new = make_ohlcv(["2024-01-02"])
        new["Close"] = 111.0
        combined = store.merge_and_write("AAPL", new)
        assert combined["Close"].iloc[0] == pytest.approx(111.0)

    def test_merge_result_is_sorted(self, tmp_data_dir):
        store = DataStore(tmp_data_dir)
        store.write("AAPL", make_ohlcv(["2024-01-05", "2024-01-04"]))
        combined = store.merge_and_write("AAPL", make_ohlcv(["2024-01-02", "2024-01-03"]))
        assert list(combined.index) == sorted(combined.index)

    def test_merge_with_no_existing_file(self, tmp_data_dir, sample_df):
        store = DataStore(tmp_data_dir)
        combined = store.merge_and_write("AAPL", sample_df)
        assert len(combined) == len(sample_df)
