"""Unit tests for downloader.market_clock.MarketClock"""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

from downloader.market_clock import MarketClock


def _mock_et(hour: int, weekday_date: date) -> MagicMock:
    m = MagicMock()
    m.hour = hour
    m.date.return_value = weekday_date
    return m


def _patch_now(et_mock):
    return patch(
        "downloader.market_clock.datetime",
        **{
            "now.return_value": et_mock,
            "combine": datetime.combine,
            "min": datetime.min,
        },
    )


class TestMarketClockWeekdays:
    def test_monday_after_close_returns_monday(self):
        with _patch_now(_mock_et(17, date(2024, 1, 8))):
            assert MarketClock.last_completed_trading_day().date() == date(2024, 1, 8)

    def test_monday_before_close_returns_friday(self):
        with _patch_now(_mock_et(10, date(2024, 1, 8))):
            assert MarketClock.last_completed_trading_day().date() == date(2024, 1, 5)

    def test_wednesday_before_close_returns_tuesday(self):
        with _patch_now(_mock_et(9, date(2024, 1, 10))):
            assert MarketClock.last_completed_trading_day().date() == date(2024, 1, 9)

    def test_wednesday_after_close_returns_wednesday(self):
        with _patch_now(_mock_et(16, date(2024, 1, 10))):
            assert MarketClock.last_completed_trading_day().date() == date(2024, 1, 10)


class TestMarketClockWeekends:
    def test_saturday_before_close_returns_friday(self):
        with _patch_now(_mock_et(10, date(2024, 1, 6))):
            assert MarketClock.last_completed_trading_day().date() == date(2024, 1, 5)

    def test_saturday_after_close_returns_friday(self):
        with _patch_now(_mock_et(17, date(2024, 1, 6))):
            assert MarketClock.last_completed_trading_day().date() == date(2024, 1, 5)

    def test_sunday_after_close_returns_friday(self):
        with _patch_now(_mock_et(17, date(2024, 1, 7))):
            assert MarketClock.last_completed_trading_day().date() == date(2024, 1, 5)


class TestMarketClockReturnType:
    def test_returns_datetime_at_midnight(self):
        with _patch_now(_mock_et(17, date(2024, 1, 8))):
            result = MarketClock.last_completed_trading_day()
        assert isinstance(result, datetime)
        assert result.hour == result.minute == result.second == 0

    def test_str_format(self):
        with _patch_now(_mock_et(17, date(2024, 1, 8))):
            assert MarketClock.last_trading_day_str() == "2024-01-08"
