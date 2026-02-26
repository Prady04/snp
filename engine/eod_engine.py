import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd

from config import DEFAULT_START_DATE, DOWNLOAD_THREADS, STATE_SAVE_INTERVAL
from downloader.market_clock import MarketClock
from downloader.yahoo import YahooDownloader
from providers.base import SymbolProvider
from storage.data_store import DataStore
from storage.state_manager import StateManager

log = logging.getLogger(__name__)


class EODEngine:
    """
    Orchestrates the full download pipeline:
      1. Gets symbols from a SymbolProvider
      2. Determines start date per symbol via StateManager (delta vs full)
      3. Downloads data in parallel via YahooDownloader
      4. Persists data via DataStore
      5. Updates StateManager after each symbol

    Call .run() for a delta update, or .run(full=True) to re-download everything.
    """

    def __init__(
        self,
        provider:   SymbolProvider,
        store:      DataStore,
        state:      StateManager,
        downloader: YahooDownloader,
    ):
        self.provider   = provider
        self.store      = store
        self.state      = state
        self.downloader = downloader

    def run(self, full: bool = False):
        symbols    = self.provider.get_symbols()
        total      = len(symbols)
        market_day = MarketClock.last_trading_day_str()

        log.info(f"EODEngine: {'FULL' if full else 'DELTA'} run -- {total} symbols, {DOWNLOAD_THREADS} threads")
        log.info(f"EODEngine: last completed trading day = {market_day}")
        log.info(f"EODEngine: data dir -> {os.path.abspath(self.store.data_dir)}")

        counts: dict[str, int]  = {"updated": 0, "skipped": 0, "failed": 0}
        failed_symbols: list[str] = []
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=DOWNLOAD_THREADS) as executor:
            futures = {
                executor.submit(self._process, sym, full): sym
                for sym in symbols
            }
            for i, future in enumerate(as_completed(futures), 1):
                sym = futures[future]
                try:
                    outcome = future.result()
                    counts[outcome] += 1
                    if outcome == "failed":
                        failed_symbols.append(sym)
                except Exception as exc:
                    counts["failed"] += 1
                    failed_symbols.append(sym)
                    log.warning(f"  {sym}: unhandled exception -- {exc}")

                if i % STATE_SAVE_INTERVAL == 0:
                    self.state.save()

                if i % 50 == 0 or i == total:
                    elapsed = time.time() - start_time
                    log.info(
                        f"  [{i}/{total}]  "
                        f"updated={counts['updated']}  "
                        f"skipped={counts['skipped']}  "
                        f"failed={counts['failed']}  "
                        f"| {i / elapsed:.1f} sym/s"
                    )

        self.state.save()
        elapsed = time.time() - start_time
        log.info("=" * 60)
        log.info(
            f"Finished in {elapsed:.1f}s | "
            f"Updated: {counts['updated']}  "
            f"Skipped: {counts['skipped']}  "
            f"Failed:  {counts['failed']}"
        )
        if failed_symbols:
            log.warning(
                f"Failed symbols ({len(failed_symbols)}): {', '.join(sorted(failed_symbols))}"
            )

    # ── private ───────────────────────────────────────────────────────────────

    def _process(self, symbol: str, full: bool) -> str:
        """Download and persist one symbol. Returns 'updated', 'skipped', or 'failed'."""
        last_csv_date   = self._last_csv_date(symbol)
        last_market_day = MarketClock.last_completed_trading_day()

        if full or last_csv_date is None or not self.store.exists(symbol):
            start = DEFAULT_START_DATE
        else:
            if last_csv_date >= last_market_day:
                log.debug(
                    f"  {symbol}: up to date "
                    f"(csv={last_csv_date.date()}, market={last_market_day.date()})"
                )
                return "skipped"
            start = (last_csv_date + timedelta(days=1)).strftime("%Y-%m-%d")

        end = (last_market_day + timedelta(days=1)).strftime("%Y-%m-%d")  # yf end is exclusive

        new_df = self.downloader.fetch(symbol, start, end)
        if new_df is None or new_df.empty:
            if self.store.exists(symbol):
                log.debug(f"  {symbol}: skipped -- no new data returned from Yahoo")
                return "skipped"
            log.warning(f"  {symbol}: FAILED -- no data returned and no existing CSV")
            return "failed"

        combined   = self.store.merge_and_write(symbol, new_df)
        last_saved = combined.index[-1].strftime("%Y-%m-%d")
        self.state.set_last_date(symbol, last_saved)
        log.debug(
            f"  {symbol}: {last_csv_date.date() if last_csv_date else 'none'} -> {last_saved}"
        )
        return "updated"

    def _last_csv_date(self, symbol: str) -> datetime | None:
        """Return the last date row from the symbol's CSV, or None."""
        if not self.store.exists(symbol):
            return None
        try:
            df = self.store.read(symbol)
            if df is None or df.empty:
                return None
            return pd.Timestamp(df.index[-1]).to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None
