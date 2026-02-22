"""
Yahoo Finance EOD Data Downloader for AmiBroker
================================================
Class hierarchy:
  SymbolProvider (abstract)
  └── SP500Provider    — fetches S&P 500 tickers from Wikipedia (7-day cache)
  StateManager         — persists last-downloaded date per symbol (JSON)
  YahooDownloader      — downloads OHLCV for one symbol with retries
  DataStore            — reads/writes per-symbol CSV files
  MarketClock          — knows what the last completed US trading day is
  EODEngine            — orchestrates everything with a thread pool
  AmibrokerExporter    — converts per-symbol CSVs to AmiBroker per-date flat files
                         with full delta support (watermark in ami_export_state.json)

Usage:
    pip install yfinance pandas lxml requests beautifulsoup4
    python yahoo_eod_downloader.py              # delta update (or full on first run)
    python yahoo_eod_downloader.py --full       # force full re-download + full export rebuild
    python yahoo_eod_downloader.py --export-only        # skip download, delta export only
    python yahoo_eod_downloader.py --export-full        # skip download, full export rebuild
"""

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("downloader.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SymbolProvider (abstract base)
# ─────────────────────────────────────────────────────────────────────────────
class SymbolProvider(ABC):
    """Returns a list of ticker symbols to download."""

    @abstractmethod
    def get_symbols(self) -> list[str]: ...


class SP500Provider(SymbolProvider):
    """
    Fetches the live S&P 500 constituent list from Wikipedia.
    Caches result for `cache_days` days to avoid hammering Wikipedia on every run.
    Dots in tickers are converted to dashes (BRK.B -> BRK-B) for Yahoo Finance.
    """

    WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    CACHE_FILENAME = "sp500_symbols.json"

    def __init__(self, cache_dir: str = "./eod_data", cache_days: int = 7):
        self.cache_path = os.path.join(cache_dir, self.CACHE_FILENAME)
        self.cache_days = cache_days
        os.makedirs(cache_dir, exist_ok=True)

    def get_symbols(self) -> list[str]:
        cached = self._load_cache()
        if cached:
            return cached
        return self._fetch_and_cache()

    def _load_cache(self) -> list[str] | None:
        if not os.path.exists(self.cache_path):
            return None
        age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(self.cache_path))
        if age > timedelta(days=self.cache_days):
            return None
        with open(self.cache_path) as f:
            data = json.load(f)
        symbols = data.get("symbols", [])
        if symbols:
            log.info(f"SP500Provider: loaded {len(symbols)} symbols from cache ({age.days}d old)")
        return symbols or None

    def _fetch_and_cache(self) -> list[str]:
        log.info("SP500Provider: fetching live list from Wikipedia...")
        try:
            import requests
            from bs4 import BeautifulSoup

            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            resp = requests.get(self.WIKIPEDIA_URL, headers=headers, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            table = soup.find("table", {"id": "constituents"})
            if table is None:
                table = soup.find("table", {"class": "wikitable"})
            if table is None:
                raise ValueError("Could not find S&P 500 table on Wikipedia page")

            headers_row = table.find("tr")
            headers_cells = [th.get_text(strip=True) for th in headers_row.find_all("th")]
            try:
                sym_idx = next(
                    i for i, h in enumerate(headers_cells)
                    if h.lower() in ("symbol", "ticker")
                )
            except StopIteration:
                sym_idx = 0

            symbols = []
            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) > sym_idx:
                    ticker = cells[sym_idx].get_text(strip=True)
                    ticker = ticker.replace(".", "-").upper()
                    if ticker:
                        symbols.append(ticker)

            if not symbols:
                raise ValueError("Parsed 0 symbols from Wikipedia table")

            log.info(f"SP500Provider: fetched {len(symbols)} symbols")
            with open(self.cache_path, "w") as f:
                json.dump(
                    {"symbols": symbols, "fetched": datetime.now().strftime("%Y-%m-%d")},
                    f,
                    indent=2,
                )
            return symbols

        except Exception as exc:
            log.error(f"SP500Provider: Wikipedia fetch failed -- {exc}")
            if os.path.exists(self.cache_path):
                log.warning("SP500Provider: using stale cache as fallback")
                with open(self.cache_path) as f:
                    return json.load(f).get("symbols", [])
            raise RuntimeError("No symbols available and no cache to fall back on.") from exc


# ─────────────────────────────────────────────────────────────────────────────
# StateManager
# ─────────────────────────────────────────────────────────────────────────────
class StateManager:
    """
    Persists the last-downloaded date for every symbol in a JSON file.
    Writes are atomic (write-to-tmp then rename) to survive crashes.
    """

    FILENAME = "download_state.json"

    def __init__(self, data_dir: str):
        self.path = os.path.join(data_dir, self.FILENAME)
        self._state: dict = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            with open(self.path) as f:
                return json.load(f)
        return {}

    def get_last_date(self, symbol: str) -> str | None:
        return self._state.get(symbol)

    def set_last_date(self, symbol: str, date_str: str):
        self._state[symbol] = date_str

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self._state, f, indent=2)
        os.replace(tmp, self.path)


# ─────────────────────────────────────────────────────────────────────────────
# DataStore
# ─────────────────────────────────────────────────────────────────────────────
class DataStore:
    """
    Reads and writes per-symbol OHLCV CSV files.
    Each file is named <SYMBOL>.csv: Date, Open, High, Low, Close, Volume.
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def csv_path(self, symbol: str) -> str:
        return os.path.join(self.data_dir, f"{symbol}.csv")

    def exists(self, symbol: str) -> bool:
        return os.path.exists(self.csv_path(symbol))

    def read(self, symbol: str):
        try:
            return pd.read_csv(self.csv_path(symbol), index_col="Date", parse_dates=True)
        except Exception:
            return None

    def write(self, symbol: str, df: pd.DataFrame):
        df.to_csv(self.csv_path(symbol))

    def merge_and_write(self, symbol: str, new_df: pd.DataFrame) -> pd.DataFrame:
        """Append new rows to existing data, dedup, sort, and save."""
        existing = self.read(symbol)
        if existing is not None:
            combined = pd.concat([existing, new_df])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined.sort_index(inplace=True)
        else:
            combined = new_df
        self.write(symbol, combined)
        return combined


# ─────────────────────────────────────────────────────────────────────────────
# YahooDownloader
# ─────────────────────────────────────────────────────────────────────────────
class YahooDownloader:
    """
    Downloads OHLCV data for a single symbol from Yahoo Finance.
    Retries on transient failures with linear back-off.
    """

    def __init__(self, retries: int = 3, retry_delay: float = 2.0):
        self.retries = retries
        self.retry_delay = retry_delay

    def fetch(self, symbol: str, start: str, end: str):
        import logging as _logging
        _logging.getLogger("yfinance").setLevel(_logging.CRITICAL)

        for attempt in range(1, self.retries + 1):
            try:
                df = yf.download(
                    symbol,
                    start=start,
                    end=end,
                    auto_adjust=True,
                    progress=False,
                )
                if df.empty:
                    return None

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
                df.index = pd.to_datetime(df.index).tz_localize(None)
                df.index.name = "Date"
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


# ─────────────────────────────────────────────────────────────────────────────
# MarketClock
# ─────────────────────────────────────────────────────────────────────────────
class MarketClock:
    """
    Knows what the last COMPLETED trading day is for US equities (NYSE/NASDAQ).
    Rules:
      - Market closes at 16:00 ET (America/New_York)
      - Before 16:00 ET today  → last completed day is the PREVIOUS trading day
      - After  16:00 ET today  → last completed day is TODAY (if a trading day)
      - Saturdays/Sundays are never trading days
      - Does not model public holidays (Yahoo silently returns no data for those,
        which is handled gracefully by the downloader)
    """

    CLOSE_HOUR_ET = 16
    TZ_ET = "America/New_York"

    @classmethod
    def last_completed_trading_day(cls) -> datetime:
        try:
            import pytz
            et_now = datetime.now(pytz.timezone(cls.TZ_ET))
        except ImportError:
            from datetime import timezone, timedelta as td
            et_now = datetime.now(timezone(td(hours=-5)))

        if et_now.hour < cls.CLOSE_HOUR_ET:
            candidate = et_now.date() - timedelta(days=1)
        else:
            candidate = et_now.date()

        while candidate.weekday() >= 5:  # 5=Sat, 6=Sun
            candidate -= timedelta(days=1)

        return datetime.combine(candidate, datetime.min.time())

    @classmethod
    def last_trading_day_str(cls) -> str:
        return cls.last_completed_trading_day().strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
# EODEngine
# ─────────────────────────────────────────────────────────────────────────────
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

    DEFAULT_START_DATE = "2010-01-01"
    THREADS = 20
    STATE_SAVE_INTERVAL = 100

    def __init__(
        self,
        provider: SymbolProvider,
        store: DataStore,
        state: StateManager,
        downloader: YahooDownloader,
    ):
        self.provider = provider
        self.store = store
        self.state = state
        self.downloader = downloader

    def run(self, full: bool = False):
        symbols = self.provider.get_symbols()
        total = len(symbols)
        market_day = MarketClock.last_trading_day_str()

        log.info(f"EODEngine: {'FULL' if full else 'DELTA'} run -- {total} symbols, {self.THREADS} threads")
        log.info(f"EODEngine: last completed trading day = {market_day}")
        log.info(f"EODEngine: data dir -> {os.path.abspath(self.store.data_dir)}")

        counts = {"updated": 0, "skipped": 0, "failed": 0}
        failed_symbols: list[str] = []
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self.THREADS) as executor:
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

                if i % self.STATE_SAVE_INTERVAL == 0:
                    self.state.save()

                if i % 50 == 0 or i == total:
                    elapsed = time.time() - start_time
                    log.info(
                        f"  [{i}/{total}] "
                        f"updated={counts['updated']} "
                        f"skipped={counts['skipped']} "
                        f"failed={counts['failed']} "
                        f"| {i / elapsed:.1f} sym/s"
                    )

        self.state.save()
        elapsed = time.time() - start_time
        log.info("=" * 60)
        log.info(
            f"Finished in {elapsed:.1f}s | "
            f"Updated: {counts['updated']} "
            f"Skipped: {counts['skipped']} "
            f"Failed: {counts['failed']}"
        )
        if failed_symbols:
            log.warning(f"Failed symbols ({len(failed_symbols)}): {', '.join(sorted(failed_symbols))}")

    def _process(self, symbol: str, full: bool) -> str:
        last_csv_date = self._last_csv_date(symbol)
        last_market_day = MarketClock.last_completed_trading_day()

        if full or last_csv_date is None or not self.store.exists(symbol):
            start = self.DEFAULT_START_DATE
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
            reason = "no data returned from Yahoo"
            outcome = "skipped" if self.store.exists(symbol) else "failed"
            if outcome == "failed":
                log.warning(f"  {symbol}: FAILED -- {reason} (no existing CSV to fall back on)")
            else:
                log.debug(f"  {symbol}: skipped -- {reason}")
            return outcome

        combined = self.store.merge_and_write(symbol, new_df)
        last_saved = combined.index[-1].strftime("%Y-%m-%d")
        self.state.set_last_date(symbol, last_saved)
        log.debug(
            f"  {symbol}: {last_csv_date.date() if last_csv_date else 'none'} -> {last_saved}"
        )
        return "updated"

    def _last_csv_date(self, symbol: str) -> datetime | None:
        if not self.store.exists(symbol):
            return None
        try:
            df = self.store.read(symbol)
            if df is None or df.empty:
                return None
            last = df.index[-1]
            return pd.Timestamp(last).to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None


# ─────────────────────────────────────────────────────────────────────────────
# AmibrokerExporter
# ─────────────────────────────────────────────────────────────────────────────
class AmibrokerExporter:
    """
    Converts per-symbol CSVs (from DataStore) into AmiBroker's flat-file format:
    one file per trading date, named YYYYMMDD.txt, containing all symbols.

    Output format (comma-separated, no header):
        SYMBOL,YYYYMMDD,Open,High,Low,Close,Volume

    Delta behaviour:
      - On first run (no state file) → full rebuild
      - On subsequent runs          → only rows after the last exported date are
                                      appended to existing date files
      - Watermark is stored in ami_export_state.json inside the output directory
      - Pass force_full=True (CLI: --export-full) to wipe and rebuild from scratch

    AmiBroker import hint:
        $FORMAT Ticker,Date_YMD,Open,High,Low,Close,Volume
        $SEPARATOR ,
        $SKIPLINES 0
    """

    DATE_FORMAT_IN  = "%Y-%m-%d"
    DATE_FORMAT_OUT = "%Y%m%d"
    BATCH_SIZE      = 25
    STATE_FILE      = "ami_export_state.json"

    def __init__(self, source_dir: str, output_dir: str):
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.state_path = os.path.join(output_dir, self.STATE_FILE)
        os.makedirs(output_dir, exist_ok=True)

    # ── state helpers ─────────────────────────────────────────────────────────

    def _load_last_exported(self) -> datetime | None:
        """Return the last date that was fully exported, or None."""
        if not os.path.exists(self.state_path):
            return None
        try:
            with open(self.state_path) as f:
                data = json.load(f)
            date_str = data.get("last_exported_date")
            return datetime.strptime(date_str, self.DATE_FORMAT_IN) if date_str else None
        except Exception as exc:
            log.warning(f"AmibrokerExporter: could not read state file -- {exc}")
            return None

    def _save_last_exported(self, dt: datetime):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(
                {"last_exported_date": dt.strftime(self.DATE_FORMAT_IN)},
                f,
                indent=2,
            )
        os.replace(tmp, self.state_path)

    # ── public ────────────────────────────────────────────────────────────────

    def run(self, force_full: bool = False):
        last_exported = self._load_last_exported()

        log.info(f"AmibrokerExporter: state_path = {os.path.abspath(self.state_path)}")
        log.info(f"AmibrokerExporter: last_exported_date loaded = {last_exported}")

        if force_full or last_exported is None:
            log.info("AmibrokerExporter: FULL export — rebuilding output directory")
            import shutil
            for item in os.listdir(self.output_dir):
                if item == self.STATE_FILE:
                    continue          # keep the state file so it isn't re-read as stale
                item_path = os.path.join(self.output_dir, item)
                if os.path.isfile(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            since_dt = None
        else:
            since_dt = last_exported + timedelta(days=1)
            log.info(
                f"AmibrokerExporter: DELTA export — "
                f"last exported={last_exported.date()}, exporting from {since_dt.date()}"
            )

        csv_files = sorted(f for f in os.listdir(self.source_dir) if f.endswith(".csv"))
        total = len(csv_files)
        log.info(f"AmibrokerExporter: {total} symbol files -> {self.output_dir}")
        log.info(f"AmibrokerExporter: processing in batches of {self.BATCH_SIZE} ...")

        processed        = 0
        skipped          = 0
        total_rows       = 0
        dates_seen: set[str]      = set()
        latest_date_seen: datetime | None = None  # watermark derived from actual data written

        for batch_start in range(0, total, self.BATCH_SIZE):
            batch = csv_files[batch_start : batch_start + self.BATCH_SIZE]
            batch_buckets, batch_rows, batch_skip, batch_latest = self._process_batch(
                batch, since_dt
            )

            processed  += len(batch) - batch_skip
            skipped    += batch_skip
            total_rows += batch_rows
            dates_seen.update(batch_buckets.keys())

            if batch_latest and (
                latest_date_seen is None or batch_latest > latest_date_seen
            ):
                latest_date_seen = batch_latest

            self._flush(batch_buckets)

            done = batch_start + len(batch)
            log.info(
                f"  [{done}/{total}] "
                f"rows_written={total_rows:,} "
                f"date_files={len(dates_seen)} "
                f"skipped={skipped}"
            )

        # ── Save watermark ────────────────────────────────────────────────────
        # Prefer the latest date actually written in this run.
        # If nothing new was written (delta, already up to date), re-save the
        # existing watermark so the state file is always present after a run.
        watermark = latest_date_seen or last_exported
        if watermark:
            self._save_last_exported(watermark)
            log.info(f"AmibrokerExporter: watermark saved -> {watermark.date()}")
        else:
            log.warning(
                "AmibrokerExporter: watermark could not be determined — "
                "state file was NOT saved. Next run will do a full rebuild."
            )

        if total_rows == 0 and since_dt is not None:
            log.info("AmibrokerExporter: nothing new to export — already up to date")

        log.info("=" * 60)
        log.info(
            f"AmibrokerExporter: done -- "
            f"{len(dates_seen)} date files, {total_rows:,} rows, "
            f"{skipped} symbols skipped"
        )
        log.info(f"Output -> {os.path.abspath(self.output_dir)}")

    # ── private ───────────────────────────────────────────────────────────────

    def _process_batch(
        self,
        fnames: list[str],
        since_dt,
    ) -> tuple[dict[str, list[str]], int, int, datetime | None]:
        """
        Read a batch of symbol CSVs.
        Returns (date_buckets, total_row_count, skipped_symbol_count, latest_date_seen).
        """
        date_buckets: dict[str, list[str]] = {}
        total_rows  = 0
        skipped     = 0
        latest_date: datetime | None = None

        for fname in fnames:
            symbol = fname[:-4]
            path   = os.path.join(self.source_dir, fname)
            try:
                df = pd.read_csv(path, index_col="Date", parse_dates=True)
            except Exception as exc:
                log.debug(f"  Skipping {symbol}: {exc}")
                skipped += 1
                continue

            if df.empty:
                skipped += 1
                continue

            if since_dt is not None:
                df = df[df.index >= since_dt]

            if df.empty:
                skipped += 1
                continue

            for date_idx, row in df.iterrows():
                try:
                    date_str = date_idx.strftime(self.DATE_FORMAT_OUT)
                    line = (
                        f"{symbol},"
                        f"{date_str},"
                        f"{row['Open']:.4f},"
                        f"{row['High']:.4f},"
                        f"{row['Low']:.4f},"
                        f"{row['Close']:.4f},"
                        f"{int(row['Volume'])}"
                    )
                    date_buckets.setdefault(date_str, []).append(line)
                    total_rows += 1

                    dt = pd.Timestamp(date_idx).to_pydatetime().replace(tzinfo=None)
                    if latest_date is None or dt > latest_date:
                        latest_date = dt

                except Exception:
                    continue

        return date_buckets, total_rows, skipped, latest_date

    def _flush(self, date_buckets: dict[str, list[str]]):
        """Append this batch's rows to the relevant date files."""
        for date_str, rows in date_buckets.items():
            out_path = os.path.join(self.output_dir, f"{date_str}.txt")
            rows.sort()
            with open(out_path, "a") as f:   # "a" = append so batches accumulate
                f.write("\n".join(rows) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
def main(full: bool = False, export_only: bool = False, export_full: bool = False):
    DATA_DIR = "./eod_data"
    AMI_DIR  = "./amibroker_data"

    if not export_only:
        engine = EODEngine(
            provider   = SP500Provider(cache_dir=DATA_DIR, cache_days=7),
            store      = DataStore(data_dir=DATA_DIR),
            state      = StateManager(data_dir=DATA_DIR),
            downloader = YahooDownloader(retries=3, retry_delay=2.0),
        )
        engine.run(full=full)

    # --full re-downloads everything but does NOT force a full export rebuild.
    # Use --export-full explicitly if you want to wipe and rebuild AmiBroker files.
    exporter = AmibrokerExporter(source_dir=DATA_DIR, output_dir=AMI_DIR)
    exporter.run(force_full=export_full)


if __name__ == "__main__":
    import sys

    main(
        full         = "--full"         in sys.argv,
        export_only  = "--export-only"  in sys.argv,
        export_full  = "--export-full"  in sys.argv,
    )
