"""Unit tests for providers.composite.CompositeProvider and providers.indexes.IndexProvider"""

from unittest.mock import MagicMock

from providers.composite import CompositeProvider
from providers.indexes import IndexProvider


def _mock_provider(symbols: list[str]):
    p = MagicMock()
    p.get_symbols.return_value = symbols
    return p


class TestCompositeProvider:
    def test_combines_symbols_from_all_providers(self):
        p = CompositeProvider([_mock_provider(["AAPL", "MSFT"]), _mock_provider(["GOOG"])])
        assert set(p.get_symbols()) == {"AAPL", "MSFT", "GOOG"}

    def test_deduplicates_across_providers(self):
        p = CompositeProvider([_mock_provider(["AAPL", "MSFT"]), _mock_provider(["MSFT", "GOOG"])])
        symbols = p.get_symbols()
        assert symbols.count("MSFT") == 1

    def test_preserves_order_first_provider_wins(self):
        p = CompositeProvider([_mock_provider(["AAPL", "MSFT"]), _mock_provider(["GOOG", "AAPL"])])
        symbols = p.get_symbols()
        assert symbols.index("AAPL") < symbols.index("GOOG")

    def test_single_provider_passthrough(self):
        p = CompositeProvider([_mock_provider(["AAPL", "MSFT"])])
        assert p.get_symbols() == ["AAPL", "MSFT"]

    def test_empty_providers_returns_empty(self):
        p = CompositeProvider([_mock_provider([])])
        assert p.get_symbols() == []

    def test_calls_every_provider(self):
        providers = [_mock_provider(["AAPL"]), _mock_provider(["MSFT"]), _mock_provider(["GOOG"])]
        CompositeProvider(providers).get_symbols()
        for prov in providers:
            prov.get_symbols.assert_called_once()


class TestIndexProvider:
    def test_returns_default_symbols(self):
        symbols = IndexProvider().get_symbols()
        assert "^GSPC" in symbols
        assert "^IXIC" in symbols
        assert "^NDX"  in symbols

    def test_custom_symbols_override_defaults(self):
        symbols = IndexProvider(["^GSPC", "^DJI"]).get_symbols()
        assert symbols == ["^GSPC", "^DJI"]
        assert "^IXIC" not in symbols

    def test_returns_list_of_strings(self):
        symbols = IndexProvider().get_symbols()
        assert isinstance(symbols, list)
        assert all(isinstance(s, str) for s in symbols)

    def test_default_symbols_start_with_caret(self):
        symbols = IndexProvider().get_symbols()
        assert all(s.startswith("^") for s in symbols)
