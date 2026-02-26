"""Unit tests for storage.state_manager.StateManager"""

import json
import os

from storage.state_manager import StateManager


class TestStateManagerLoad:
    def test_starts_empty_when_no_file(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir)
        assert sm.get_last_date("AAPL") is None

    def test_loads_existing_state_file(self, tmp_data_dir):
        with open(os.path.join(tmp_data_dir, "download_state.json"), "w") as f:
            json.dump({"AAPL": "2024-01-08", "MSFT": "2024-01-07"}, f)
        sm = StateManager(tmp_data_dir)
        assert sm.get_last_date("AAPL") == "2024-01-08"
        assert sm.get_last_date("MSFT") == "2024-01-07"


class TestStateManagerGetSet:
    def test_set_and_get_roundtrip(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir)
        sm.set_last_date("GOOG", "2024-03-15")
        assert sm.get_last_date("GOOG") == "2024-03-15"

    def test_get_unknown_symbol_returns_none(self, tmp_data_dir):
        assert StateManager(tmp_data_dir).get_last_date("UNKNOWN") is None

    def test_overwrite_existing_date(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir)
        sm.set_last_date("AAPL", "2024-01-01")
        sm.set_last_date("AAPL", "2024-06-01")
        assert sm.get_last_date("AAPL") == "2024-06-01"


class TestStateManagerSave:
    def test_save_creates_file(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir)
        sm.set_last_date("AAPL", "2024-01-08")
        sm.save()
        assert os.path.exists(sm.path)

    def test_save_persists_to_disk(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir)
        sm.set_last_date("AAPL", "2024-01-08")
        sm.save()
        assert StateManager(tmp_data_dir).get_last_date("AAPL") == "2024-01-08"

    def test_save_is_atomic_no_tmp_left_behind(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir)
        sm.set_last_date("AAPL", "2024-01-08")
        sm.save()
        assert not os.path.exists(sm.path + ".tmp")

    def test_save_multiple_symbols(self, tmp_data_dir):
        sm = StateManager(tmp_data_dir)
        sm.set_last_date("AAPL", "2024-01-08")
        sm.set_last_date("MSFT", "2024-01-07")
        sm.set_last_date("GOOG", "2024-01-05")
        sm.save()
        sm2 = StateManager(tmp_data_dir)
        assert sm2.get_last_date("MSFT") == "2024-01-07"
        assert sm2.get_last_date("GOOG") == "2024-01-05"
