"""Unit tests for engine.eod_engine.EODEngine"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import make_ohlcv
from engine.eod_engine import EODEngine
from storage.data_store import DataStore
from storage.state_manager import StateManager


MARKET_DAY = datetime(2024, 1, 8)


def _make_engine(tmp_data_dir, downloader_return=None, symbols=None):
    provider           = MagicMock()
    provider.get_symbols.return_value = symbols or ["AAPL"]
    downloader         = MagicMock()
    downloader.fetch.return_value = downloader_return
    store              = DataStore(tmp_data_dir)
    state              = StateManager(tmp_data_dir)
    engine             = EODEngine(provider=provider, store=store, state=state, downloader=downloader)
    return engine, store, state, downloader


def _patch_market_day(dt: datetime = MARKET_DAY):
    m = MagicMock()
    m.last_completed_trading_day.return_value = dt
    m.last_trading_day_str.return_value = dt.strftime("%Y-%m-%d")
    return patch("engine.eod_engine.MarketClock", m)


class TestEODEngineProcessFreshSymbol:
    def test_new_symbol_downloads_from_default_start(self, tmp_data_dir):
        engine, store, state, downloader = _make_engine(tmp_data_dir, make_ohlcv(["2024-01-08"]))
        with _patch_market_day():
            assert engine._process("AAPL", full=False) == "updated"
        assert downloader.fetch.call_args[0][1] == "2010-01-01"

    def test_new_symbol_with_no_data_returns_failed(self, tmp_data_dir):
        engine, *_ = _make_engine(tmp_data_dir, downloader_return=None)
        with _patch_market_day():
            assert engine._process("AAPL", full=False) == "failed"

    def test_new_symbol_creates_csv(self, tmp_data_dir):
        engine, store, *_ = _make_engine(tmp_data_dir, make_ohlcv(["2024-01-08"]))
        with _patch_market_day():
            engine._process("AAPL", full=False)
        assert store.exists("AAPL")


class TestEODEngineProcessDelta:
    def test_up_to_date_symbol_is_skipped(self, tmp_data_dir):
        engine, store, state, downloader = _make_engine(tmp_data_dir, downloader_return=None)
        store.write("AAPL", make_ohlcv(["2024-01-08"]))
        with _patch_market_day(datetime(2024, 1, 8)):
            assert engine._process("AAPL", full=False) == "skipped"
        downloader.fetch.assert_not_called()

    def test_stale_symbol_fetches_from_day_after_last_csv(self, tmp_data_dir):
        engine, store, state, downloader = _make_engine(tmp_data_dir, make_ohlcv(["2024-01-08"]))
        store.write("AAPL", make_ohlcv(["2024-01-05"]))
        with _patch_market_day(datetime(2024, 1, 8)):
            assert engine._process("AAPL", full=False) == "updated"
        assert downloader.fetch.call_args[0][1] == "2024-01-06"

    def test_stale_symbol_no_new_data_returns_skipped(self, tmp_data_dir):
        engine, store, *_ = _make_engine(tmp_data_dir, downloader_return=None)
        store.write("AAPL", make_ohlcv(["2024-01-05"]))
        with _patch_market_day(datetime(2024, 1, 8)):
            assert engine._process("AAPL", full=False) == "skipped"


class TestEODEngineProcessFull:
    def test_full_mode_always_starts_from_default_start(self, tmp_data_dir):
        engine, store, state, downloader = _make_engine(tmp_data_dir, make_ohlcv(["2024-01-08"]))
        store.write("AAPL", make_ohlcv(["2024-01-08"]))
        with _patch_market_day():
            assert engine._process("AAPL", full=True) == "updated"
        assert downloader.fetch.call_args[0][1] == "2010-01-01"


class TestEODEngineStateUpdate:
    def test_state_updated_after_successful_download(self, tmp_data_dir):
        engine, store, state, _ = _make_engine(tmp_data_dir, make_ohlcv(["2024-01-08"]))
        with _patch_market_day():
            engine._process("AAPL", full=False)
        assert state.get_last_date("AAPL") == "2024-01-08"

    def test_state_not_updated_when_skipped(self, tmp_data_dir):
        engine, store, state, _ = _make_engine(tmp_data_dir, downloader_return=None)
        store.write("AAPL", make_ohlcv(["2024-01-08"]))
        with _patch_market_day(datetime(2024, 1, 8)):
            engine._process("AAPL", full=False)
        assert state.get_last_date("AAPL") is None


class TestEODEngineRun:
    def test_run_processes_all_symbols(self, tmp_data_dir):
        engine, store, state, downloader = _make_engine(
            tmp_data_dir, make_ohlcv(["2024-01-08"]), symbols=["AAPL", "MSFT", "GOOG"]
        )
        with _patch_market_day():
            engine.run(full=False)
        assert downloader.fetch.call_count == 3

    def test_run_saves_state_file(self, tmp_data_dir):
        import os
        engine, store, state, _ = _make_engine(tmp_data_dir, make_ohlcv(["2024-01-08"]))
        with _patch_market_day():
            engine.run(full=False)
        assert os.path.exists(state.path)
