import logging
import time

import pandas as pd

from config import DOWNLOAD_RETRIES, DOWNLOAD_RETRY_DELAY

log = logging.getLogger(__name__)

try:
    import yfinance as yf
except ImportError as exc:
    raise ImportError(
        "yfinance is required. Install it with:  pip install yfinance"
    ) from exc


class YahooDownloader:
    """
    Downloads OHLCV data for a single symbol from Yahoo Finance.
    Retries on transient failures with linear back-off.

    Uses yf.Ticker().history() rather than yf.download() to guarantee a clean
    single-symbol DataFrame regardless of ticker type (equity or index).
    yf.download() with a single symbol still returns a MultiIndex in newer
    yfinance versions, which causes column misalignment for index tickers (^GSPC etc.).

    Index symbols have no real Volume — Yahoo returns 0 or NaN.
    Volume is filled with 0 so rows are never silently dropped.
    """

    def __init__(
        self,
        retries: int       = DOWNLOAD_RETRIES,
        retry_delay: float = DOWNLOAD_RETRY_DELAY,
    ):
        self.retries     = retries
        self.retry_delay = retry_delay

    def fetch(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        """
        Download OHLCV bars for `symbol` between `start` and `end` (exclusive).
        Returns a DataFrame indexed by Date, or None on failure.
        """
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)

        for attempt in range(1, self.retries + 1):
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(
                    start=start,
                    end=end,
                    auto_adjust=True,
                    actions=False,      # exclude dividends/splits columns
                )

                if df.empty:
                    return None

                # history() always returns flat columns — no MultiIndex to handle
                df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
                df.index = pd.to_datetime(df.index).tz_localize(None)
                df.index.name = "Date"

                # Index symbols (^GSPC etc.) have no meaningful volume.
                # Fill NaN with 0 so int() never raises on missing volume.
                df["Volume"] = df["Volume"].fillna(0).astype(int)

                return df.round(4)

            except Exception as exc:
                if attempt < self.retries:
                    log.debug(
                        f"YahooDownloader: {symbol} attempt {attempt}/{self.retries} "
                        f"failed ({exc}), retrying in {self.retry_delay * attempt:.1f}s"
                    )
                    time.sleep(self.retry_delay * attempt)
                else:
                    log.warning(
                        f"YahooDownloader: {symbol} FAILED after {self.retries} attempts "
                        f"-- {type(exc).__name__}: {exc}"
                    )
                    return None
