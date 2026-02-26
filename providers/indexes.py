import logging

from providers.base import SymbolProvider

log = logging.getLogger(__name__)


class IndexProvider(SymbolProvider):
    """
    Returns a static list of major US market index symbols.

    These are downloaded directly from Yahoo Finance using their caret (^) prefix.
    Add or remove entries in INDEX_SYMBOLS to customise which indexes are tracked.

    Common Yahoo Finance index tickers:
        ^GSPC   S&P 500 index
        ^IXIC   Nasdaq Composite
        ^NDX    Nasdaq-100
        ^DJI    Dow Jones Industrial Average
        ^RUT    Russell 2000
        ^VIX    CBOE Volatility Index
    """

    INDEX_SYMBOLS: list[str] = [
        "^GSPC",   # S&P 500
        "^IXIC",   # Nasdaq Composite
        "^NDX",    # Nasdaq-100
        "^DJI",    # Dow Jones
        "^RUT",    # Russell 2000
        "^VIX",    # VIX
    ]

    def __init__(self, symbols: list[str] | None = None):
        """
        Pass a custom list of index symbols to override the defaults.
        Example: IndexProvider(["^GSPC", "^IXIC"])
        """
        self._symbols = symbols if symbols is not None else self.INDEX_SYMBOLS

    def get_symbols(self) -> list[str]:
        log.info(f"IndexProvider: {len(self._symbols)} index symbols: {', '.join(self._symbols)}")
        return list(self._symbols)
