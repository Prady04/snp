import json
import logging
import os
from datetime import datetime, timedelta

from providers.base import SymbolProvider
from config import DATA_DIR, SP500_CACHE_DAYS

log = logging.getLogger(__name__)


class SP500Provider(SymbolProvider):
    """
    Fetches the live S&P 500 constituent list from Wikipedia.
    Caches result for `cache_days` days to avoid hammering Wikipedia on every run.
    Dots in tickers are converted to dashes (BRK.B -> BRK-B) for Yahoo Finance.
    """

    WIKIPEDIA_URL  = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    CACHE_FILENAME = "sp500_symbols.json"

    def __init__(self, cache_dir: str = DATA_DIR, cache_days: int = SP500_CACHE_DAYS):
        self.cache_path = os.path.join(cache_dir, self.CACHE_FILENAME)
        self.cache_days = cache_days
        os.makedirs(cache_dir, exist_ok=True)

    def get_symbols(self) -> list[str]:
        cached = self._load_cache()
        if cached:
            return cached
        return self._fetch_and_cache()

    # ── private ───────────────────────────────────────────────────────────────

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

            headers_row   = table.find("tr")
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
