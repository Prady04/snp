"""Unit tests for providers.sp500.SP500Provider — no network calls."""

import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest

from providers.sp500 import SP500Provider


WIKI_HTML = """
<html><body>
<table id="constituents">
  <tr><th>Symbol</th><th>Security</th></tr>
  <tr><td>AAPL</td><td>Apple</td></tr>
  <tr><td>MSFT</td><td>Microsoft</td></tr>
  <tr><td>BRK.B</td><td>Berkshire</td></tr>
</table>
</body></html>
"""


def _mock_response(html: str = WIKI_HTML):
    resp = MagicMock()
    resp.text = html
    resp.raise_for_status = MagicMock()
    return resp


class TestSP500ProviderCache:
    def test_writes_cache_after_fetch(self, tmp_data_dir):
        with patch("providers.sp500.requests.get", return_value=_mock_response()):
            SP500Provider(cache_dir=tmp_data_dir).get_symbols()
        assert os.path.exists(os.path.join(tmp_data_dir, "sp500_symbols.json"))

    def test_reads_from_cache_on_second_call(self, tmp_data_dir):
        with patch("providers.sp500.requests.get", return_value=_mock_response()) as mock_get:
            p = SP500Provider(cache_dir=tmp_data_dir)
            p.get_symbols()
            p.get_symbols()
        assert mock_get.call_count == 1

    def test_expired_cache_triggers_refetch(self, tmp_data_dir):
        cache_path = os.path.join(tmp_data_dir, "sp500_symbols.json")
        with open(cache_path, "w") as f:
            json.dump({"symbols": ["OLD"], "fetched": "2020-01-01"}, f)
        os.utime(cache_path, (time.time() - 10 * 86400,) * 2)

        with patch("providers.sp500.requests.get", return_value=_mock_response()) as mock_get:
            symbols = SP500Provider(cache_dir=tmp_data_dir, cache_days=7).get_symbols()

        assert mock_get.call_count == 1
        assert "OLD" not in symbols

    def test_stale_cache_used_as_fallback_on_network_error(self, tmp_data_dir):
        cache_path = os.path.join(tmp_data_dir, "sp500_symbols.json")
        with open(cache_path, "w") as f:
            json.dump({"symbols": ["FALLBACK"], "fetched": "2020-01-01"}, f)
        os.utime(cache_path, (time.time() - 10 * 86400,) * 2)

        with patch("providers.sp500.requests.get", side_effect=ConnectionError("down")):
            symbols = SP500Provider(cache_dir=tmp_data_dir, cache_days=7).get_symbols()

        assert symbols == ["FALLBACK"]

    def test_raises_when_no_cache_and_network_fails(self, tmp_data_dir):
        with patch("providers.sp500.requests.get", side_effect=ConnectionError("down")):
            with pytest.raises(RuntimeError, match="No symbols available"):
                SP500Provider(cache_dir=tmp_data_dir).get_symbols()


class TestSP500ProviderParsing:
    def test_parses_symbols_from_html(self, tmp_data_dir):
        with patch("providers.sp500.requests.get", return_value=_mock_response()):
            symbols = SP500Provider(cache_dir=tmp_data_dir).get_symbols()
        assert "AAPL" in symbols and "MSFT" in symbols

    def test_converts_dots_to_dashes(self, tmp_data_dir):
        with patch("providers.sp500.requests.get", return_value=_mock_response()):
            symbols = SP500Provider(cache_dir=tmp_data_dir).get_symbols()
        assert "BRK-B" in symbols
        assert "BRK.B" not in symbols

    def test_symbols_are_uppercase(self, tmp_data_dir):
        html = WIKI_HTML.replace("AAPL", "aapl")
        with patch("providers.sp500.requests.get", return_value=_mock_response(html)):
            symbols = SP500Provider(cache_dir=tmp_data_dir).get_symbols()
        assert "AAPL" in symbols

    def test_raises_when_table_not_found(self, tmp_data_dir):
        bad_html = "<html><body><p>No table here</p></body></html>"
        with patch("providers.sp500.requests.get", return_value=_mock_response(bad_html)):
            with pytest.raises(RuntimeError):
                SP500Provider(cache_dir=tmp_data_dir).get_symbols()

    def test_returns_list_of_strings(self, tmp_data_dir):
        with patch("providers.sp500.requests.get", return_value=_mock_response()):
            symbols = SP500Provider(cache_dir=tmp_data_dir).get_symbols()
        assert isinstance(symbols, list)
        assert all(isinstance(s, str) for s in symbols)
