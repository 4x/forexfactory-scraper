# sandbox.py
import logging
from forexfactory.scraper import scrape_range_pandas
import nodriver as uc
from datetime import datetime, timedelta
from dateutil.tz import gettz

from forexfactory.utils.logging import configure_logging

if __name__ == "__main__":
    configure_logging()
    logger = logging.getLogger(__name__)
    logging.getLogger("nodriver").setLevel(logging.WARNING) # way too verbose
    # logging.getLogger("pandas").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)

    print('Hello')
    tz = gettz(datetime.now().astimezone().tzname())
    from_date = datetime.fromisoformat("2025-09-11").replace(tzinfo=tz)
    to_date = datetime.fromisoformat("2025-09-11").replace(tzinfo=tz)

    result = uc.loop().run_until_complete(scrape_range_pandas(
        from_date=from_date, to_date=to_date, output_csv="erase_me.csv"))
    print(result)
