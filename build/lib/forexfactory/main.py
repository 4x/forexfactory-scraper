"""CLI entry point for ForexFactory scraper."""
import logging
import argparse
from datetime import datetime, timedelta
import asyncio
from .scraper import scrape_range_pandas
from .utils.logging import configure_logging


async def main():
    """Main CLI entry point."""
    configure_logging()
    logger = logging.getLogger(__name__)
    logging.getLogger("nodriver").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)

    today = datetime.today()
    parser = argparse.ArgumentParser(description="ForexFactory Calendar Scraper")
    parser.add_argument('--start', type=str, required=True,
        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, required=True,
        help='End date (YYYY-MM-DD)')
    parser.add_argument('--csv', type=str, default="forex_factory_cache.csv",
        help='Output CSV file')
    parser.add_argument('--details', action='store_true', default=False,
        help='Scrape event details')

    args = parser.parse_args()

    from_date = datetime.fromisoformat(args.start)
    to_date = datetime.fromisoformat(args.end)

    logger.info(f"Scraping {args.start} to {args.end}")
    df = await scrape_range_pandas(from_date, to_date,
        output_csv=args.csv, scrape_details=args.details)
    logger.info(f"Scraped {len(df)} events")


if __name__ == "__main__":
    asyncio.run(main())
