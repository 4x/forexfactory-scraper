# src/forexfactory/scraper.py

import asyncio
import re
import logging
import pandas as pd
from datetime import datetime, timedelta
import nodriver as uc

from .csv_util import ensure_csv_header, read_existing_data, write_data_to_csv, merge_new_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def detail_data_to_string(detail_data: dict) -> str:
    """
    Convert dictionary from parse_detail_table() into a single string for CSV storage.
    Replace newlines or excessive whitespaces with space.
    """
    parts = []
    for k, v in detail_data.items():
        # Replacing all whitespace (including \n, \r, tabs) with a single space
        k_clean = re.sub(r'\s+', ' ', k).strip()
        v_clean = re.sub(r'\s+', ' ', v).strip()
        parts.append(f"{k_clean}: {v_clean}")
    return " | ".join(parts)


async def parse_detail_table(detail_element):
    """
    Parses the detail table from a nodriver element.
    Returns a dictionary of specs.
    """
    detail_data = {}
    try:
        detail_table = await detail_element.select('.//table[@class="calendarspecs"]')
        rows = await detail_table.select_all('./tr')
        for r in rows:
            try:
                spec_name_element = await r.select('./td[1]')
                spec_desc_element = await r.select('./td[2]')
                spec_name = await spec_name_element.get_text()
                spec_desc = await spec_desc_element.get_text()
                detail_data[spec_name.strip()] = spec_desc.strip()
            except Exception:
                pass
    except Exception as e:
        logger.error("Error parsing detail table: %s", e)
    return detail_data


async def parse_calendar_day(page, the_date: datetime, scrape_details=False, existing_df=None) -> pd.DataFrame:
    """
    Scrape data for a single day (the_date) and return a DataFrame with columns:
      DateTime, Currency, Impact, Event, Actual, Forecast, Previous, Detail
    If scrape_details is False, skip detail parsing.

    Before fetching detail data from the Internet, this function checks if the record
    already exists (using existing_df) with a non-empty "Detail" field.
    """
    date_str = the_date.strftime('%b%d.%Y').lower()
    url = f"https://www.forexfactory.com/calendar?day={date_str}"
    logger.info(f"Scraping URL: {url}")
    await page.get(url)

    try:
        await page.select('//table[contains(@class,"calendar__table")]', timeout=15)
    except Exception:
        logger.warning(f"Page did not load for day={the_date.date()}")
        return pd.DataFrame(
            columns=["DateTime", "Currency", "Impact", "Event", "Actual", "Forecast", "Previous", "Detail"])

    rows = await page.select_all('//tr[contains(@class,"calendar__row")]')
    data_list = []
    current_day = the_date

    for row in rows:
        row_class = await row.get_attribute("class")
        if "day-breaker" in row_class or "no-event" in row_class:
            continue

        # Parse the basic cells
        try:
            time_el = await row.select('.//td[contains(@class,"calendar__time")]')
            currency_el = await row.select('.//td[contains(@class,"calendar__currency")]')
            impact_el = await row.select('.//td[contains(@class,"calendar__impact")]')
            event_el = await row.select('.//td[contains(@class,"calendar__event")]')
            actual_el = await row.select('.//td[contains(@class,"calendar__actual")]')
            forecast_el = await row.select('.//td[contains(@class,"calendar__forecast")]')
            previous_el = await row.select('.//td[contains(@class,"calendar__previous")]')
        except Exception:
            continue

        time_text = (await time_el.get_text()).strip()
        currency_text = (await currency_el.get_text()).strip()

        # Get impact text
        impact_text = ""
        try:
            impact_span = await impact_el.select('.//span')
            impact_text = await impact_span.get_attribute("title") or ""
        except Exception:
            impact_text = (await impact_el.get_text()).strip()

        event_text = (await event_el.get_text()).strip()
        actual_text = (await actual_el.get_text()).strip()
        forecast_text = (await forecast_el.get_text()).strip()
        previous_text = (await previous_el.get_text()).strip()

        # Determine event time based on text
        event_dt = current_day
        time_lower = time_text.lower()
        if "day" in time_lower:
            event_dt = event_dt.replace(hour=23, minute=59, second=59)
        elif "data" in time_lower:
            event_dt = event_dt.replace(hour=0, minute=0, second=1)
        else:
            m = re.match(r'(\d{1,2}):(\d{2})(am|pm)', time_lower)
            if m:
                hh = int(m.group(1))
                mm = int(m.group(2))
                ampm = m.group(3)
                if ampm == 'pm' and hh < 12:
                    hh += 12
                if ampm == 'am' and hh == 12:
                    hh = 0
                event_dt = event_dt.replace(hour=hh, minute=mm, second=0)

        # Compute a unique key for the event using DateTime, Currency, and Event
        unique_key = f"{event_dt.isoformat()}_{currency_text}_{event_text}"

        # Initialize detail string
        detail_str = ""
        if scrape_details:
            # If an existing CSV DataFrame is provided, check if this record exists and has detail.
            if existing_df is not None:
                matched = existing_df[
                    (existing_df["DateTime"] == event_dt.isoformat()) &
                    (existing_df["Currency"].str.strip() == currency_text) &
                    (existing_df["Event"].str.strip() == event_text)
                    ]
                if not matched.empty:
                    existing_detail = str(matched.iloc[0]["Detail"]).strip() if pd.notnull(
                        matched.iloc[0]["Detail"]) else ""
                    if existing_detail:
                        detail_str = existing_detail

            # If detail_str is still empty, then fetch detail from the Internet.
            if not detail_str:
                try:
                    open_link = await row.select('.//td[contains(@class,"calendar__detail")]/a')
                    await open_link.scroll_into_view()
                    await asyncio.sleep(1)
                    await open_link.click()
                    detail_element = await page.select('//tr[contains(@class,"calendar__details--detail")]', timeout=5)

                    detail_data = await parse_detail_table(detail_element)
                    detail_str = detail_data_to_string(detail_data)

                    try:
                        close_link = await row.select('.//a[@title="Close Detail"]')
                        await close_link.click()
                    except Exception:
                        pass
                except Exception:
                    pass

        data_list.append({
            "DateTime": event_dt.isoformat(),
            "Currency": currency_text,
            "Impact": impact_text,
            "Event": event_text,
            "Actual": actual_text,
            "Forecast": forecast_text,
            "Previous": previous_text,
            "Detail": detail_str
        })

    return pd.DataFrame(data_list)


async def scrape_day(page, the_date: datetime, existing_df: pd.DataFrame, scrape_details=False) -> pd.DataFrame:
    """
    Re-scrape a single day, using existing_df to check for already-saved details.
    """
    df_day_new = await parse_calendar_day(page, the_date, scrape_details=scrape_details, existing_df=existing_df)
    return df_day_new


async def scrape_range_pandas(from_date: datetime, to_date: datetime, output_csv: str, tzname="Asia/Tehran",
                        scrape_details=False):
    from .csv_util import ensure_csv_header, read_existing_data, merge_new_data, write_data_to_csv

    ensure_csv_header(output_csv)
    existing_df = read_existing_data(output_csv)

    # browser = await uc.start(browser_executable_path="/home/jules/.cache/ms-playwright/chromium-1181/chrome-linux/chrome", no_sandbox=True, browser_args=['--disable-gpu', '--headless'])
    # await page.set_window_size(1400, 1000)
    browser = await uc.start()
    page = await browser.get('about:blank')

    total_new = 0
    day_count = (to_date - from_date).days + 1
    logger.info(f"Scraping from {from_date.date()} to {to_date.date()} for {day_count} days.")

    try:
        current_day = from_date
        while current_day <= to_date:
            logger.info(f"Scraping day {current_day.strftime('%Y-%m-%d')}...")
            df_new = await scrape_day(page, current_day, existing_df, scrape_details=scrape_details)

            if not df_new.empty:
                merged_df = merge_new_data(existing_df, df_new)
                new_rows = len(merged_df) - len(existing_df)
                if new_rows > 0:
                    logger.info(f"Added/Updated {new_rows} rows for {current_day.date()}")
                existing_df = merged_df
                total_new += new_rows

                # Save updated data to CSV after processing the day's data.
                write_data_to_csv(existing_df, output_csv)

            current_day += timedelta(days=1)
    finally:
        if browser:
            try:
                await browser.close()
                logger.info("Chrome WebDriver closed successfully.")
            except Exception as e:
                logger.error(f"Error closing WebDriver: {e}")
            finally:
                browser = None

    # Final save (if needed)
    write_data_to_csv(existing_df, output_csv)
    logger.info(f"Done. Total new/updated rows: {total_new}")
