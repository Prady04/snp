import json
import logging
import os
import re
from datetime import datetime, timedelta

import pandas as pd

from config import AMI_BATCH_SIZE, AMI_DIR

log = logging.getLogger(__name__)

# Reverse the DataStore safe-filename encoding to recover the original symbol.
# _GSPC.csv -> ^GSPC,  AAPL.csv -> AAPL
_SAFE_PREFIX = re.compile(r'^_')


def _filename_to_symbol(fname_stem: str) -> str:
    """
    Convert a CSV filename stem back to its original Yahoo Finance symbol.
    _GSPC  -> ^GSPC
    BRK-B  -> BRK-B
    """
    return _SAFE_PREFIX.sub("^", fname_stem)


def _symbol_to_ami(symbol: str) -> str:
    """
    Convert a Yahoo Finance symbol to an AmiBroker-safe ticker.
    AmiBroker does not support ^ in ticker names.
    ^GSPC  -> GSPC
    ^IXIC  -> IXIC
    AAPL   -> AAPL
    """
    return symbol.lstrip("^")


class AmibrokerExporter:
    """
    Converts per-symbol CSVs into AmiBroker's flat-file format:
    one file per trading date, named YYYYMMDD.txt, containing all symbols.

    Output format (comma-separated, no header):
        TICKER,YYYYMMDD,Open,High,Low,Close,Volume

    Index tickers have ^ stripped for AmiBroker compatibility:
        ^GSPC -> GSPC,20240102,...
        ^IXIC -> IXIC,20240102,...

    Delta behaviour:
      - On first run (no state file) → full rebuild
      - On subsequent runs           → only rows after the last exported date
                                       are appended to existing date files
      - Watermark stored in ami_export_state.json inside the output directory
      - Pass force_full=True (CLI: --export-full) to wipe and rebuild from scratch

    AmiBroker import hint:
        $FORMAT Ticker,Date_YMD,Open,High,Low,Close,Volume
        $SEPARATOR ,
        $SKIPLINES 0
    """

    DATE_FORMAT_IN  = "%Y-%m-%d"
    DATE_FORMAT_OUT = "%Y%m%d"
    STATE_FILE      = "ami_export_state.json"

    def __init__(self, source_dir: str, output_dir: str = AMI_DIR):
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.state_path = os.path.join(output_dir, self.STATE_FILE)
        os.makedirs(output_dir, exist_ok=True)

    # ── state helpers ─────────────────────────────────────────────────────────

    def _load_last_exported(self) -> datetime | None:
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
            json.dump({"last_exported_date": dt.strftime(self.DATE_FORMAT_IN)}, f, indent=2)
        os.replace(tmp, self.state_path)

    # ── public ────────────────────────────────────────────────────────────────

    def run(self, force_full: bool = False):
        last_exported = self._load_last_exported()

        log.info(f"AmibrokerExporter: state_path    = {os.path.abspath(self.state_path)}")
        log.info(f"AmibrokerExporter: last_exported = {last_exported}")

        if force_full or last_exported is None:
            log.info("AmibrokerExporter: FULL export — rebuilding output directory")
            import shutil
            for item in os.listdir(self.output_dir):
                if item == self.STATE_FILE:
                    continue
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
        total     = len(csv_files)
        log.info(f"AmibrokerExporter: {total} symbol files -> {self.output_dir}")
        log.info(f"AmibrokerExporter: processing in batches of {AMI_BATCH_SIZE} ...")

        processed         = 0
        skipped           = 0
        total_rows        = 0
        dates_seen: set[str]       = set()
        latest_date_seen: datetime | None = None

        for batch_start in range(0, total, AMI_BATCH_SIZE):
            batch = csv_files[batch_start : batch_start + AMI_BATCH_SIZE]
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
                f"  [{done}/{total}]  "
                f"rows_written={total_rows:,}  "
                f"date_files={len(dates_seen)}  "
                f"skipped={skipped}"
            )

        # Always save watermark — if nothing new, re-save existing so state file persists
        watermark = latest_date_seen or last_exported
        if watermark:
            self._save_last_exported(watermark)
            log.info(f"AmibrokerExporter: watermark saved -> {watermark.date()}")
        else:
            log.warning(
                "AmibrokerExporter: watermark could not be determined — "
                "state file NOT saved; next run will do a full rebuild"
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
        since_dt: datetime | None,
    ) -> tuple[dict[str, list[str]], int, int, datetime | None]:
        """
        Read a batch of symbol CSVs.
        Returns (date_buckets, total_row_count, skipped_count, latest_date_seen).
        """
        date_buckets: dict[str, list[str]] = {}
        total_rows  = 0
        skipped     = 0
        latest_date: datetime | None = None

        for fname in fnames:
            fname_stem = fname[:-4]                          # strip .csv
            yf_symbol  = _filename_to_symbol(fname_stem)    # _GSPC -> ^GSPC
            ami_ticker = _symbol_to_ami(yf_symbol)          # ^GSPC -> GSPC

            path = os.path.join(self.source_dir, fname)
            try:
                df = pd.read_csv(path, index_col="Date", parse_dates=True)
            except Exception as exc:
                log.debug(f"  Skipping {yf_symbol}: {exc}")
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
                    # Volume: indexes have 0, equities have real volume — both are safe int
                    volume = int(row["Volume"]) if pd.notna(row["Volume"]) else 0
                    line = (
                        f"{ami_ticker},"
                        f"{date_str},"
                        f"{row['Open']:.4f},"
                        f"{row['High']:.4f},"
                        f"{row['Low']:.4f},"
                        f"{row['Close']:.4f},"
                        f"{volume}"
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
            with open(out_path, "a") as f:
                f.write("\n".join(rows) + "\n")
