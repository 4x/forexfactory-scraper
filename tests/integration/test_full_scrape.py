"""Integration tests for ForexFactory scraper."""
import os
import unittest
from datetime import datetime

from forexfactory.scraper import scrape_range_pandas
from forexfactory.event import CalendarEvent, Impact


class TestFullScrape(unittest.IsolatedAsyncioTestCase):
    """Integration tests that actually scrape ForexFactory."""

    def setUp(self):
        self.output_file = "test_integration_output.csv"
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    def tearDown(self):
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    async def test_scrape_single_day(self):
        """Test scraping a single day returns valid DataFrame."""
        # Use a date in the future that should have events
        start_dt = datetime(2025, 11, 24)
        end_dt = datetime(2025, 11, 24)

        df = await scrape_range_pandas(
            from_date=start_dt,
            to_date=end_dt,
            output_csv=self.output_file,
            scrape_details=False
        )

        # Should have events
        self.assertGreater(len(df), 0, "Should have at least one event")

        # Check DataFrame columns
        expected_cols = ["DateTime", "Currency", "Impact", "Event",
                        "Actual", "Forecast", "Previous", "Detail"]
        self.assertEqual(list(df.columns), expected_cols)

        # Check DateTime is timezone-aware (contains offset)
        first_dt = df['DateTime'].iloc[0]
        self.assertIn('+', first_dt, f"DateTime should have timezone: {first_dt}")

        # Check CSV was created
        self.assertTrue(os.path.exists(self.output_file))

    async def test_scrape_returns_dataframe_without_csv(self):
        """Test scraping without saving to CSV."""
        start_dt = datetime(2025, 11, 24)
        end_dt = datetime(2025, 11, 24)

        df = await scrape_range_pandas(
            from_date=start_dt,
            to_date=end_dt,
            scrape_details=False
        )

        self.assertGreater(len(df), 0)
        self.assertFalse(os.path.exists(self.output_file))


if __name__ == '__main__':
    unittest.main()
