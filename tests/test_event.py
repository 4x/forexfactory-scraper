"""Unit tests for event module."""
import unittest
from datetime import datetime, timezone

from forexfactory.event import (
    CalendarEvent, Impact, normalize_impact, parse_time_to_datetime
)
from forex_common import Currency


class TestNormalizeImpact(unittest.TestCase):
    """Tests for normalize_impact function."""

    def test_high_impact(self):
        self.assertEqual(normalize_impact("High Impact Expected"), Impact.HIGH)
        self.assertEqual(normalize_impact("high"), Impact.HIGH)

    def test_medium_impact(self):
        self.assertEqual(normalize_impact("Medium Impact Expected"), Impact.MEDIUM)
        self.assertEqual(normalize_impact("medium"), Impact.MEDIUM)

    def test_low_impact(self):
        self.assertEqual(normalize_impact("Low Impact Expected"), Impact.LOW)
        self.assertEqual(normalize_impact("low"), Impact.LOW)

    def test_holiday(self):
        self.assertEqual(normalize_impact("Non-Economic"), Impact.HOLIDAY)
        self.assertEqual(normalize_impact("holiday"), Impact.HOLIDAY)

    def test_unknown(self):
        self.assertEqual(normalize_impact(""), Impact.UNKNOWN)
        self.assertEqual(normalize_impact("something else"), Impact.UNKNOWN)


class TestParseTimeToDatetime(unittest.TestCase):
    """Tests for parse_time_to_datetime function."""

    def setUp(self):
        self.base_date = datetime(2025, 11, 24)

    def test_am_time(self):
        result = parse_time_to_datetime("8:30am", self.base_date)
        self.assertEqual(result.hour, 8)
        self.assertEqual(result.minute, 30)

    def test_pm_time(self):
        result = parse_time_to_datetime("2:45pm", self.base_date)
        self.assertEqual(result.hour, 14)
        self.assertEqual(result.minute, 45)

    def test_noon(self):
        result = parse_time_to_datetime("12:00pm", self.base_date)
        self.assertEqual(result.hour, 12)

    def test_midnight(self):
        result = parse_time_to_datetime("12:00am", self.base_date)
        self.assertEqual(result.hour, 0)

    def test_all_day(self):
        result = parse_time_to_datetime("All Day", self.base_date)
        self.assertEqual(result.hour, 0)
        self.assertEqual(result.minute, 0)


class TestCalendarEvent(unittest.TestCase):
    """Tests for CalendarEvent dataclass."""

    def test_create_event(self):
        event = CalendarEvent(
            time=datetime(2025, 11, 24, 8, 30, tzinfo=timezone.utc),
            currency=Currency(symbol="USD"),
            impact=Impact.HIGH,
            event="Non-Farm Payrolls"
        )
        self.assertEqual(event.event, "Non-Farm Payrolls")
        self.assertEqual(event.impact, Impact.HIGH)

    def test_optional_fields(self):
        event = CalendarEvent(
            time=datetime(2025, 11, 24, tzinfo=timezone.utc),
            currency=Currency(symbol="EUR"),
            impact=Impact.MEDIUM,
            event="German ifo",
            forecast="88.6",
            previous="88.4"
        )
        self.assertEqual(event.forecast, "88.6")
        self.assertEqual(event.previous, "88.4")
        self.assertIsNone(event.actual)


if __name__ == '__main__':
    unittest.main()
