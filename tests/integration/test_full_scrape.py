import sys
import os
import asyncio
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import unittest
from datetime import datetime
from dateutil.tz import gettz

from src.forexfactory.incremental import scrape_incremental

class TestFullScrape(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # در صورت نیاز، محیط تست را آماده می‌کنیم
        self.output_file = "test_integration_output.csv"
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    def tearDown(self):
        # پاکسازی نهایی اگر لازم باشد
        if os.path.exists(self.output_file):
            os.remove(self.output_file)

    async def test_scrape_small_range(self):
        """
        یک تست انتها به انتها که یک بازه کوچک را اسکرپ می‌کند
        و بررسی می‌کند آیا فایل CSV تولید شده و حاوی سطر(های) مورد انتظار هست یا خیر.
        """
        tz = gettz("Asia/Tehran")
        start_dt = datetime(2025, 1, 5, tzinfo=tz)
        end_dt   = datetime(2025, 1, 5, tzinfo=tz)
        await scrape_incremental(
            from_date=start_dt,
            to_date=end_dt,
            output_csv=self.output_file,
            tzname="Asia/Tehran",
            scrape_details=True
        )

        # حالا بررسی می‌کنیم آیا فایل تولید شده و آیا حداقل یک رویداد ثبت شده است
        self.assertTrue(os.path.exists(self.output_file), "CSV output file should be created.")

        with open(self.output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            self.assertGreater(len(lines), 1, "Should have at least one row of data (plus header).")
            # می‌توانید سطرها را پارس کنید و مطمئن شوید که بعضی اطلاعات کلیدی داریم
            # e.g. lines[1] must contain "USD" or "FOMC"
            # یا اینکه بسنجید که Detail خالی نباشد.

if __name__ == '__main__':
    unittest.main()
