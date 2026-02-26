from datetime import datetime, timedelta

from config import MARKET_CLOSE_HOUR_ET


class MarketClock:
    """
    Knows what the last COMPLETED trading day is for US equities (NYSE/NASDAQ).

    Rules:
      - Market closes at MARKET_CLOSE_HOUR_ET (default 16:00) Eastern Time
      - Before close today  → last completed day is the PREVIOUS trading day
      - After  close today  → last completed day is TODAY (if a trading day)
      - Saturdays/Sundays are never trading days
      - Does not model public holidays (Yahoo silently returns no data for those,
        which is handled gracefully by the downloader)
    """

    TZ_ET = "America/New_York"

    @classmethod
    def last_completed_trading_day(cls) -> datetime:
        """Return the last date for which a full trading day is available."""
        try:
            import pytz
            et_now = datetime.now(pytz.timezone(cls.TZ_ET))
        except ImportError:
            # pytz not installed — fall back to UTC-5 (ET standard, close enough)
            from datetime import timezone, timedelta as td
            et_now = datetime.now(timezone(td(hours=-5)))

        candidate = (
            et_now.date() - timedelta(days=1)
            if et_now.hour < MARKET_CLOSE_HOUR_ET
            else et_now.date()
        )

        # Roll back past weekends
        while candidate.weekday() >= 5:  # 5=Sat, 6=Sun
            candidate -= timedelta(days=1)

        return datetime.combine(candidate, datetime.min.time())

    @classmethod
    def last_trading_day_str(cls) -> str:
        return cls.last_completed_trading_day().strftime("%Y-%m-%d")
