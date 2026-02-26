"""
Yahoo Finance EOD Data Downloader for AmiBroker
================================================
Entry point — wires all components together and handles CLI arguments.

Usage:
    pip install yfinance pandas requests beautifulsoup4 lxml
    python main.py                    # delta download + delta export
    python main.py --full             # full re-download + delta export
    python main.py --export-only      # skip download, delta export only
    python main.py --export-full      # skip download, full export rebuild
    python main.py --purge-indexes    # delete stale index CSVs then re-download
"""

import logging
import os
import re
import sys

import config
from downloader.yahoo import YahooDownloader
from engine.eod_engine import EODEngine
from export.amibroker import AmibrokerExporter
from providers.composite import CompositeProvider
from providers.indexes import IndexProvider
from providers.nasdaq import NasdaqProvider
from providers.sp500 import SP500Provider
from storage.data_store import DataStore, _safe_filename
from storage.state_manager import StateManager


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("downloader.log"),
            logging.StreamHandler(),
        ],
    )


def build_provider() -> CompositeProvider:
    """
    Build the symbol provider from whichever sources are enabled in config.
    Toggle DOWNLOAD_SP500 / DOWNLOAD_NASDAQ / DOWNLOAD_INDEXES in config.py.
    """
    providers = []
    if config.DOWNLOAD_SP500:
        providers.append(
            SP500Provider(cache_dir=config.DATA_DIR, cache_days=config.SP500_CACHE_DAYS)
        )
    if config.DOWNLOAD_NASDAQ:
        providers.append(
            NasdaqProvider(cache_dir=config.DATA_DIR, cache_days=config.SP500_CACHE_DAYS)
        )
    if config.DOWNLOAD_INDEXES:
        providers.append(IndexProvider())

    if not providers:
        raise RuntimeError(
            "No symbol sources enabled. "
            "Set at least one of DOWNLOAD_SP500 / DOWNLOAD_NASDAQ / DOWNLOAD_INDEXES "
            "to True in config.py."
        )
    return CompositeProvider(providers)


def purge_index_files():
    """
    Delete the CSV and state entries for all index symbols so they are
    re-downloaded cleanly on the next run.
    Fixes stale data written by older versions of the downloader.
    """
    log = logging.getLogger(__name__)
    index_symbols = IndexProvider().get_symbols()   # e.g. ['^GSPC', '^IXIC', ...]
    state = StateManager(data_dir=config.DATA_DIR)
    purged = 0

    for sym in index_symbols:
        csv_path = os.path.join(config.DATA_DIR, f"{_safe_filename(sym)}.csv")
        if os.path.exists(csv_path):
            os.remove(csv_path)
            log.info(f"  purged: {csv_path}")
            purged += 1
        else:
            log.info(f"  not found (skipped): {csv_path}")

        # Also clear the state so delta logic doesn't skip re-download
        if state.get_last_date(sym):
            state.set_last_date(sym, "")   # blank forces re-download

    state.save()
    log.info(f"Purged {purged} index CSV files — run without --purge-indexes to re-download.")


def main(
    full: bool          = False,
    export_only: bool   = False,
    export_full: bool   = False,
    purge_indexes: bool = False,
):
    setup_logging()

    if purge_indexes:
        purge_index_files()
        return   # just purge; user re-runs normally to download fresh data

    if not export_only:
        engine = EODEngine(
            provider   = build_provider(),
            store      = DataStore(data_dir=config.DATA_DIR),
            state      = StateManager(data_dir=config.DATA_DIR),
            downloader = YahooDownloader(
                retries     = config.DOWNLOAD_RETRIES,
                retry_delay = config.DOWNLOAD_RETRY_DELAY,
            ),
        )
        engine.run(full=full)

    exporter = AmibrokerExporter(source_dir=config.DATA_DIR, output_dir=config.AMI_DIR)
    exporter.run(force_full=export_full)


if __name__ == "__main__":
    main(
        full          = "--full"          in sys.argv,
        export_only   = "--export-only"   in sys.argv,
        export_full   = "--export-full"   in sys.argv,
        purge_indexes = "--purge-indexes" in sys.argv,
    )
