"""Unit tests for export.amibroker.AmibrokerExporter"""

import json
import os
from datetime import datetime

import pytest

from tests.conftest import make_ohlcv
from export.amibroker import AmibrokerExporter
from storage.data_store import DataStore


# ── helpers ───────────────────────────────────────────────────────────────────

def _write_csv(data_dir: str, symbol: str, dates: list[str]):
    DataStore(data_dir).write(symbol, make_ohlcv(dates, symbol=symbol))


def _read_date_file(ami_dir: str, date_str: str) -> list[str]:
    path = os.path.join(ami_dir, f"{date_str}.txt")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [l for l in f.read().splitlines() if l.strip()]


def _write_watermark(ami_dir: str, date_str: str):
    with open(os.path.join(ami_dir, "ami_export_state.json"), "w") as f:
        json.dump({"last_exported_date": date_str}, f)


# ── full export ───────────────────────────────────────────────────────────────

class TestAmibrokerExporterFullExport:
    def test_creates_date_files_for_each_trading_day(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02", "2024-01-03"])
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        assert len(_read_date_file(tmp_ami_dir, "20240102")) == 1
        assert len(_read_date_file(tmp_ami_dir, "20240103")) == 1

    def test_line_format_is_correct(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02"])
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        parts = _read_date_file(tmp_ami_dir, "20240102")[0].split(",")
        assert parts[0] == "AAPL"
        assert parts[1] == "20240102"
        assert len(parts) == 7

    def test_multiple_symbols_in_same_date_file(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02"])
        _write_csv(tmp_data_dir, "MSFT", ["2024-01-02"])
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        tickers = [l.split(",")[0] for l in _read_date_file(tmp_ami_dir, "20240102")]
        assert "AAPL" in tickers and "MSFT" in tickers

    def test_date_file_lines_sorted_alphabetically(self, tmp_data_dir, tmp_ami_dir):
        for sym in ["MSFT", "AAPL", "GOOG"]:
            _write_csv(tmp_data_dir, sym, ["2024-01-02"])
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        tickers = [l.split(",")[0] for l in _read_date_file(tmp_ami_dir, "20240102")]
        assert tickers == sorted(tickers)

    def test_full_export_wipes_existing_date_files(self, tmp_data_dir, tmp_ami_dir):
        stale = os.path.join(tmp_ami_dir, "20230101.txt")
        with open(stale, "w") as f:
            f.write("OLD,20230101,1,1,1,1,100\n")
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02"])
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        assert not os.path.exists(stale)

    def test_full_export_preserves_state_file(self, tmp_data_dir, tmp_ami_dir):
        _write_watermark(tmp_ami_dir, "2024-01-01")
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02"])
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        assert os.path.exists(os.path.join(tmp_ami_dir, "ami_export_state.json"))


# ── watermark / state ─────────────────────────────────────────────────────────

class TestAmibrokerExporterWatermark:
    def test_watermark_written_after_full_export(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02", "2024-01-03"])
        exp = AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir)
        exp.run(force_full=True)
        assert exp._load_last_exported().strftime("%Y-%m-%d") == "2024-01-03"

    def test_watermark_re_saved_when_nothing_new(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02"])
        _write_watermark(tmp_ami_dir, "2024-01-02")
        exp = AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir)
        exp.run(force_full=False)
        assert exp._load_last_exported().strftime("%Y-%m-%d") == "2024-01-02"

    def test_load_returns_none_when_no_state_file(self, tmp_ami_dir):
        exp = AmibrokerExporter(source_dir="/nonexistent", output_dir=tmp_ami_dir)
        assert exp._load_last_exported() is None

    def test_load_returns_none_on_corrupt_state(self, tmp_ami_dir):
        with open(os.path.join(tmp_ami_dir, "ami_export_state.json"), "w") as f:
            f.write("NOT JSON{{{")
        exp = AmibrokerExporter(source_dir="/nonexistent", output_dir=tmp_ami_dir)
        assert exp._load_last_exported() is None

    def test_save_is_atomic(self, tmp_ami_dir):
        exp = AmibrokerExporter(source_dir="/nonexistent", output_dir=tmp_ami_dir)
        exp._save_last_exported(datetime(2024, 1, 8))
        assert not os.path.exists(exp.state_path + ".tmp")
        assert os.path.exists(exp.state_path)


# ── delta export ──────────────────────────────────────────────────────────────

class TestAmibrokerExporterDelta:
    def test_delta_only_exports_new_dates(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02", "2024-01-03", "2024-01-04"])
        _write_watermark(tmp_ami_dir, "2024-01-03")
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=False)
        assert _read_date_file(tmp_ami_dir, "20240102") == []
        assert _read_date_file(tmp_ami_dir, "20240103") == []
        assert len(_read_date_file(tmp_ami_dir, "20240104")) == 1

    def test_delta_does_not_duplicate_on_repeat_run(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02", "2024-01-03"])
        exp = AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir)
        exp.run(force_full=True)
        exp.run(force_full=False)   # nothing new
        assert len(_read_date_file(tmp_ami_dir, "20240102")) == 1

    def test_force_full_wipes_and_rebuilds(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02"])
        exp = AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir)
        exp.run(force_full=True)
        exp.run(force_full=True)
        assert len(_read_date_file(tmp_ami_dir, "20240102")) == 1


# ── edge cases ────────────────────────────────────────────────────────────────

class TestAmibrokerExporterEdgeCases:
    def test_empty_source_dir_produces_no_date_files(self, tmp_data_dir, tmp_ami_dir):
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        assert [f for f in os.listdir(tmp_ami_dir) if f.endswith(".txt")] == []

    def test_skips_non_csv_files_in_source_dir(self, tmp_data_dir, tmp_ami_dir):
        with open(os.path.join(tmp_data_dir, "download_state.json"), "w") as f:
            f.write("{}")
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02"])
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        assert len(_read_date_file(tmp_ami_dir, "20240102")) == 1

    def test_ohlcv_values_formatted_to_4dp(self, tmp_data_dir, tmp_ami_dir):
        store = DataStore(tmp_data_dir)
        df = make_ohlcv(["2024-01-02"])
        df["Close"] = 123.456789
        store.write("AAPL", df)
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        close_val = _read_date_file(tmp_ami_dir, "20240102")[0].split(",")[5]
        assert close_val == "123.4568"

    def test_volume_is_integer(self, tmp_data_dir, tmp_ami_dir):
        _write_csv(tmp_data_dir, "AAPL", ["2024-01-02"])
        AmibrokerExporter(source_dir=tmp_data_dir, output_dir=tmp_ami_dir).run(force_full=True)
        volume = _read_date_file(tmp_ami_dir, "20240102")[0].split(",")[6]
        assert "." not in volume
