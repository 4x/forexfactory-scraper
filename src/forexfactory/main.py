# src/forexfactory/main.py

import logging
import argparse
from datetime import datetime, timedelta
from dateutil.tz import gettz
import nodriver as uc

from .incremental import scrape_incremental
from forexfactory.utils.logging import configure_logging

async def main():
    configure_logging()
    logger = logging.getLogger(__name__)
    logging.getLogger("nodriver").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("pandas").setLevel(logging.WARNING)

    today = datetime.today()
    parser = argparse.ArgumentParser(description="Forex Factory Scraper (Incremental + pandas)")
    parser.add_argument('--start', type=str, required=True,
        help='Start date (YYYY-MM-DD)', default=today.strftime('%Y-%m-%d'))
    parser.add_argument('--end', type=str, required=True,
        help='End date (YYYY-MM-DD)', default=(today+timedelta(weeks=1)).strftime('%Y-%m-%d'))
    parser.add_argument('--csv', type=str, default="forex_factory_cache.csv", help='Output CSV file')
    parser.add_argument('--tz', type=str,
        default=datetime.now().astimezone().tzname(), help='Timezone')
    parser.add_argument('--details', action='store_false', default=True,
        help='Scrape details or not')

    args = parser.parse_args()

    tz = gettz(args.tz)
    from_date = datetime.fromisoformat(args.start).replace(tzinfo=tz)
    to_date = datetime.fromisoformat(args.end).replace(tzinfo=tz)

    await scrape_incremental(from_date, to_date, args.csv, tzname=args.tz, scrape_details=args.details)

if __name__ == "__main__":
    uc.loop().run_until_complete(main())