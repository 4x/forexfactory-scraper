import asyncio
import re
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
import nodriver as uc
from forex_common import Currency
from .utils.csv_util import ensure_csv_header, read_existing_data, merge_new_data, write_data_to_csv
from .event import CalendarEvent, Impact, normalize_impact, parse_time_to_datetime

logger = logging.getLogger(__name__)

# main -> incremental.scrape_incremental -> scrape_range_pandas -> scrape_day ->
# parse_calendar_day
# --------------------
# Wrappers / orchestration
# --------------------

def events_to_dataframe(events: list[CalendarEvent]) -> pd.DataFrame:
    """Convert list of CalendarEvents to DataFrame."""
    if not events:
        return pd.DataFrame(columns=["DateTime", "Currency", "Impact", "Event",
                                     "Actual", "Forecast", "Previous", "Detail"])

    data = []
    for e in events:
        data.append({
            "DateTime": e.time.isoformat(),
            "Currency": e.currency.symbol if hasattr(e.currency, 'symbol') else str(e.currency),
            "Impact": e.impact.name if e.impact != Impact.UNKNOWN else "",
            "Event": e.event,
            "Actual": e.actual or "",
            "Forecast": e.forecast or "",
            "Previous": e.previous or "",
            "Detail": e.detail or ""
        })

    return pd.DataFrame(data)


async def scrape_range_pandas(from_date: datetime, to_date: datetime,
    output_csv: str = None, scrape_details: bool = False) -> pd.DataFrame:
    """
    Scrape ForexFactory calendar for date range and return DataFrame of CalendarEvents.

    Args:
        from_date: Start date
        to_date: End date
        output_csv: Optional CSV file to save results
        scrape_details: Whether to scrape event details

    Returns:
        DataFrame with columns: DateTime, Currency, Impact, Event, Actual, Forecast, Previous, Detail
        DateTime is timezone-aware in local time.
    """
    if output_csv:
        ensure_csv_header(output_csv)
        existing_df = read_existing_data(output_csv)
    else:
        existing_df = pd.DataFrame(columns=["DateTime", "Currency", "Impact", "Event",
                                            "Actual", "Forecast", "Previous", "Detail"])

    browser = await uc.start()
    page = await browser.get('about:blank')

    all_events: list[CalendarEvent] = []
    day_count = (to_date - from_date).days + 1
    logger.info(f"Scraping from {from_date.date()} to {to_date.date()} for {day_count} days.")

    try:
        current_day = from_date
        while current_day <= to_date:
            events = await scrape_day(page, current_day, existing_df,
                                      scrape_details=scrape_details)

            if events:
                all_events.extend(events)
                df_new = events_to_dataframe(events)

                if output_csv:
                    merged_df = merge_new_data(existing_df, df_new)
                    new_rows = len(merged_df) - len(existing_df)
                    if new_rows > 0:
                        logger.info(f"Added/Updated {new_rows} rows for {current_day.date()}")
                    existing_df = merged_df
                    write_data_to_csv(existing_df, output_csv)

            current_day += timedelta(days=1)
    finally:
        if browser:
            async def _try_call_shutdown(obj):
                # Try a list of common shutdown/close method names used by various drivers
                for name in ("close", "quit", "stop", "shutdown", "disconnect"):
                    func = getattr(obj, name, None)
                    if not func:
                        continue
                    try:
                        res = func()
                        # If the call returned a coroutine, await it
                        if asyncio.iscoroutine(res):
                            await res
                        logger.info(f"[nodriver] {name} called successfully.")
                        return
                    except Exception as exc:
                        logger.warning(f"Attempt to call nodriver.{name}() \
                                       raised: {exc}")
                logger.warning(
                "No supported shutdown method succeeded 4 WebDriver instance.")

            try:
                await _try_call_shutdown(browser)
            except Exception as e:
                logger.error(f"Error closing nodriver: {e}")
            finally:
                browser = None

    logger.info(f"Done. Total events scraped: {len(all_events)}")

    # Return DataFrame of all events
    return events_to_dataframe(all_events)


async def scrape_day(page, the_date: datetime, existing_df: pd.DataFrame,
                     scrape_details=False) -> list[CalendarEvent]:
    """
    Scrape a single day, returning list of CalendarEvents.
    """
    events = await parse_calendar_day(page, the_date,
        scrape_details=scrape_details, existing_df=existing_df)

    # Handle case where parse_calendar_day returns empty DataFrame on error
    if isinstance(events, pd.DataFrame):
        return []

    return events

# --------------------
# Main day parser
# --------------------
async def parse_calendar_day(page, the_date: datetime,
            scrape_details=False, existing_df=None) -> pd.DataFrame:
    """
    Scrape data for a single day (the_date) and return a DataFrame with columns:
      DateTime, Currency, Impact, Event, Actual, Forecast, Previous, Detail

    This function first tries to use nodriver's select/select_all. If those raise a DOM/Protocol
    exception (e.g. '-32000'), it falls back to running JS via page.evaluate(...) to
    collect the visible rows as a serializable list of dicts. Detail scraping (if requested)
    is handled via evaluate as well (click via JS, then extract the detail table).
    """
    date_str = the_date.strftime('%b%d.%Y').lower()
    url = f"https://www.forexfactory.com/calendar?day={date_str}"
    logger.debug(f"Scraping {url}")
    await page.get(url)

    # small delay to let JS start running (helps with race conditions)
    await asyncio.sleep(0.35)

    # ---- helper: robust wait for calendar table (tries select, then fallback evaluate)
    async def _wait_for_calendar_table_and_get_rows(page, url, max_attempts=3):
                # xpath='//table[contains(@class,"calendar__table")]'):
        last_exc = None
        for attempt in range(1, max_attempts + 1):
            try: # normal fast path first
                if attempt > 1:
                    await asyncio.sleep(min(0.5 * (2 ** (attempt - 2)), 2.0))
                # this may raise ProtocolException if CDP dom query fails
                nodes = await page.select_all('//tr[contains(@class,"calendar__row")]')                
                if not nodes: # normalize: some drivers return None
                    nodes = []
                return {"mode": "elements", "nodes": nodes}
            except Exception as exc:
                last_exc = exc
                msg = str(exc)
                logger.warning("Waiting for calendar table attempt %d/%d failed: %s", attempt, max_attempts, msg)
                # if it looks like the CDP DOM error, try a page reload/get once
                if ('DOM Error' in msg) or ('-32000' in msg) or (
                    'Execution context' in msg) or (
                        'context' in msg and 'destroyed' in msg):
                    try:
                        reload_fn = getattr(page, "reload", None)
                        if reload_fn:
                            res = reload_fn()
                            if asyncio.iscoroutine(res):
                                await res
                        else:
                            await page.get(url)
                        logger.info("Tried page.reload()/re-get after DOM error.")                        
                        await asyncio.sleep(0.5) # small pause after reload
                    except Exception:
                        logger.debug("reload/get attempt failed",exc_info=True)
                # Next: try the JS fallback to directly pull row data from the page's runtime
                try:
                    js = r"""
                    (() => {
                        const rows = Array.from(document.querySelectorAll('tr.calendar__row'));
                        // Extract header time for timezone detection
                        const headerTimeEl = document.querySelector('.calendar__header .calendar__time');
                        const headerTime = headerTimeEl ? headerTimeEl.innerText.trim() : '';

                        const rowsData = rows.map(r => {
                            const cls = r.className || '';
                            const q = sel => {
                                const el = r.querySelector(sel);
                                return el ? el.innerText.trim() : '';
                            };
                            const getSpanTitle = (sel) => {
                                const sp = r.querySelector(sel);
                                if (!sp) return '';
                                if (sp.getAttribute) {
                                    return sp.getAttribute('title') || (sp.innerText || '').trim();
                                }
                                return (sp.innerText || '').trim();
                            };
                            return {
                                className: cls,
                                time: q('td.calendar__time'),
                                currency: q('td.calendar__currency'),
                                impact: getSpanTitle('td.calendar__impact span') || q('td.calendar__impact'),
                                event: q('td.calendar__event'),
                                actual: q('td.calendar__actual'),
                                forecast: q('td.calendar__forecast'),
                                previous: q('td.calendar__previous'),
                                hasDetail: !!r.querySelector('td.calendar__detail a')
                            };
                        });
                        return { rows: rowsData, headerTime: headerTime };
                    })();
                    """
                    result = await page.evaluate(js)
                    logger.debug(f"JS evaluate result type: {type(result)}, first 200 chars: {str(result)[:200]}")

                    # Handle nested format from nodriver
                    # nodriver returns JS objects as lists of [key, value] pairs
                    rows_data = []
                    header_time = ""

                    if isinstance(result, list) and result and isinstance(result[0], list):
                        # Format: [['rows', {...}], ['headerTime', {...}]]
                        result_dict = {k: v for k, v in result}
                        rows_obj = result_dict.get("rows", {})
                        header_obj = result_dict.get("headerTime", {})

                        # Extract rows array
                        if isinstance(rows_obj, dict) and rows_obj.get("type") == "array":
                            rows_data = rows_obj.get("value", [])
                        elif isinstance(rows_obj, list):
                            rows_data = rows_obj

                        # Extract header time
                        if isinstance(header_obj, dict) and "value" in header_obj:
                            header_time = header_obj.get("value", "")
                        elif isinstance(header_obj, str):
                            header_time = header_obj
                    elif isinstance(result, dict):
                        rows_data = result.get("rows", [])
                        header_time = result.get("headerTime", "")

                    logger.debug(f"Header time from FF: {header_time}, rows count: {len(rows_data) if isinstance(rows_data, list) else 'N/A'}")

                    # if JS returned rows, use it
                    if isinstance(rows_data, list) and len(rows_data) > 0:
                        return {"mode": "js", "rows_data": rows_data, "header_time": header_time}
                    else:
                        logger.debug("JS did not return row list.")
                except Exception as e2:
                    logger.debug("JS fallback evaluate attempt %d failed: %s",
                                 attempt, e2, exc_info=True)

                # continue retry loop
                continue

        # if we got here, nothing succeeded. attempt to dump the page HTML to disk for debugging
        try:
            logger.debug('Nothing succeeded: attempting to dump page')
            page_html = None
            for attr in ("get_content", "get_html",
                         "content", "page_source", "source"):
                fn = getattr(page, attr, None)
                if fn:
                    try:
                        res = fn()
                        if asyncio.iscoroutine(res):
                            res = await res
                        page_html = str(res)
                        break
                    except Exception:
                        continue
            if not page_html:
                try:
                    page_html = await page.evaluate("() => document.documentElement.outerHTML")
                except Exception:
                    page_html = None
            if page_html:
                fname = f"forexfactory_debug_{the_date.strftime('%Y%m%d')}.html"
                with open(fname, "w", encoding="utf-8") as fh:
                    fh.write(page_html[:300000])
                logger.warning("Saved debug HTML snapshot to %s (truncated).", fname)
        except Exception:
            logger.debug("Failed to dump page HTML 4 debugging", exc_info=True)
        raise last_exc or RuntimeError("Failed waiting for calendar table.")

    # call helper
    try:
        rows_result = await _wait_for_calendar_table_and_get_rows(page, url)
    except Exception as e:
        logger.warning(f"Extraction did not work for {the_date.date()}: {e}", exc_info=True)
        return pd.DataFrame(columns=["DateTime", "Currency", "Impact", "Event",
            "Actual", "Forecast", "Previous", "Detail"])

    # ----------------------------------------------------
    # Extract data using the appropriate mode
    # ----------------------------------------------------
    current_day = the_date
    header_time = rows_result.get("header_time", "")
    logger.debug("Found %d rows for %s using mode %s",
        len(rows_result.get("nodes", []))
        if "nodes" in rows_result else len(rows_result.get("rows_data", [])),
        the_date.date(),
        rows_result["mode"])

    if rows_result["mode"] == "elements":
        events = await extract_via_elements(rows_result["nodes"],
            current_day, scrape_details, existing_df, page, header_time)
    else: # JS mode
        events = await extract_via_javascript(rows_result["rows_data"],
            current_day, scrape_details, existing_df, page, header_time)

    return events

# --------------------
# Helper utilities
# --------------------
async def _normalize_element(el):
    """If selector returns a list-like, return the first element. If None, return None."""
    if el is None:
        return None
    # many drivers return lists for select_all/select; normalize:
    try:
        # duck-type: lists/tuples
        if isinstance(el, (list, tuple)) and len(el) > 0:
            return el[0]
    except Exception:
        pass
    return el

async def safe_select(element, xpath, timeout=None):
    """Safe wrapper for element.select() that returns a single normalized element or None."""
    try:
        node = await element.select(xpath, timeout=timeout)  # driver-specific API
        return await _normalize_element(node)
    except Exception:
        return None

async def safe_select_all(element, xpath):
    """Safe wrapper for element.select_all() that returns list (maybe empty)."""
    try:
        nodes = await element.select_all(xpath)
        if nodes is None:
            return []
        return nodes
    except Exception:
        return []

async def safe_text(element):
    """Return stripped text or empty string. Accepts element or list."""
    try:
        if element is None:
            return ""
        el = await _normalize_element(element)
        txt = await el.get_text()
        return txt.strip() if txt else ""
    except Exception:
        return ""

async def safe_attribute(element, attr_name):
    """Return attribute value or empty string. Accepts element or list."""
    try:
        if element is None:
            return ""
        el = await _normalize_element(element)
        val = await el.get_attribute(attr_name)
        return val or ""
    except Exception:
        return ""


# --------------------
# Detail parsing
# --------------------
async def parse_detail_table(detail_element):
    """
    Parses the detail table from a nodriver element.
    Returns a dictionary of specs.
    """
    detail_data = {}
    try:
        detail_element = await _normalize_element(detail_element)
        if not detail_element:
            return detail_data

        detail_table = await safe_select(detail_element, './/table[@class="calendarspecs"]')
        if not detail_table:
            return detail_data

        rows = await safe_select_all(detail_table, './tr')
        for r in rows:
            try:
                spec_name_el = await safe_select(r, './td[1]')
                spec_desc_el = await safe_select(r, './td[2]')
                spec_name = await safe_text(spec_name_el)
                spec_desc = await safe_text(spec_desc_el)
                if spec_name:
                    detail_data[spec_name.strip()] = spec_desc.strip()
            except Exception:
                logger.debug("Ignored one detail row due to parse issue", exc_info=True)
                continue
    except Exception as e:
        logger.error("Error parsing detail table: %s", e, exc_info=True)
    return detail_data

async def parse_event_details(page, row_or_index, event_dt: datetime, currency_text: str, 
                            event_text: str, existing_df, mode: str = "elements") -> str:
    """
    Extract event details for a given row.
    
    Args:
        page: Browser page object
        row_or_index: Either a row element (elements mode) or row index (js mode)
        event_dt: Event datetime
        currency_text: Currency code
        event_text: Event name
        existing_df: DataFrame of existing data to check for cached details
        mode: "elements" or "js"
    
    Returns:
        Detail string or empty string if no details found
    """

    def _detail_data_to_string(detail_data: dict) -> str:
        """
        Convert dictionary from parse_detail_table() into a single string for CSV storage.
        Replace newlines or excessive whitespaces with space.
        """
        parts = []
        for k, v in detail_data.items():
            k_clean = re.sub(r'\s+', ' ', k).strip()
            v_clean = re.sub(r'\s+', ' ', v).strip()
            parts.append(f"{k_clean}: {v_clean}")
        return " | ".join(parts)

    detail_str = ""
    
    try:
        # Check existing_df first to avoid re-scraping
        if existing_df is not None:
            matched = existing_df[
                (existing_df["DateTime"] == event_dt.isoformat()) &
                (existing_df["Currency"].str.strip() == currency_text) &
                (existing_df["Event"].str.strip() == event_text)
            ]
            if not matched.empty:
                existing_detail = str(matched.iloc[0]["Detail"]).strip() if pd.notnull(matched.iloc[0]["Detail"]) else ""
                if existing_detail:
                    return existing_detail

        if mode == "elements":
            # Element-based detail extraction
            open_link = await row_or_index.select('.//td[contains(@class,"calendar__detail")]/a')
            if open_link:
                try:
                    await open_link.scroll_into_view()
                except Exception:
                    pass
                await asyncio.sleep(0.25)
                try:
                    await open_link.click()
                except Exception:
                    logger.debug("click() on detail link failed (elements path)", exc_info=True)

                # Extract detail via element selector
                try:
                    detail_element = await page.select('//tr[contains(@class,"calendar__details--detail")]', timeout=3)
                    detail_data = await parse_detail_table(detail_element)
                    detail_str = _detail_data_to_string(detail_data)
                except Exception:
                    logger.debug("Couldn't read detail element after click (elements path)", exc_info=True)

                # Close detail panel
                try:
                    close_link = await page.select('.//a[@title="Close Detail"]')
                    close_link = await _normalize_element(close_link)
                    if close_link:
                        try:
                            await close_link.click()
                        except Exception:
                            pass
                except Exception:
                    pass

        elif mode == "js":
            # JavaScript-based detail extraction
            idx = row_or_index  # In JS mode, this is the row index
            js_click = f"""
            (() => {{
                const rows = Array.from(document.querySelectorAll('tr.calendar__row'));
                if (!rows || rows.length <= {idx}) return false;
                const link = rows[{idx}].querySelector('td.calendar__detail a');
                if (!link) return false;
                link.scrollIntoView();
                link.click();
                return true;
            }})();
            """
            try:
                clicked = await page.evaluate(js_click)
                if clicked:
                    # Wait for detail row to appear
                    await asyncio.sleep(0.45)
                    js_detail = r"""
                    (() => {
                        const out = {};
                        const detailRow = document.querySelector('tr.calendar__details--detail');
                        if (!detailRow) return null;
                        const table = detailRow.querySelector('table.calendarspecs');
                        if (!table) return null;
                        Array.from(table.querySelectorAll('tr')).forEach(tr => {
                            const tds = tr.querySelectorAll('td');
                            if (tds.length >= 2) {
                                const k = (tds[0].innerText || '').trim();
                                const v = (tds[1].innerText || '').trim();
                                if (k) out[k] = v;
                            }
                        });
                        return out;
                    })();
                    """
                    detail_data = await page.evaluate(js_detail)
                    if isinstance(detail_data, dict):
                        detail_str = _detail_data_to_string(detail_data)
                    # Close detail panel
                    try:
                        await page.evaluate("""() => { const c = document.querySelector('a[title="Close Detail"]'); if (c){ c.click(); return true } return false }""")
                    except Exception:
                        pass
            except Exception:
                logger.debug("JS detail click/extract failed for idx %d", idx, exc_info=True)

    except Exception:
        logger.debug(f"Detail extraction error ({mode} path)", exc_info=True)
    
    return detail_str

async def extract_via_elements(rows, current_day: datetime, scrape_details: bool,
                             existing_df, page, header_time: str = "") -> list:
    """
    Extract calendar data using nodriver element handles.
    
    Args:
        rows: List of row elements
        current_day: Base date for the calendar day
        scrape_details: Whether to extract event details
        existing_df: DataFrame of existing data
        page: Browser page object
    
    Returns:
        List of event dictionaries
    """
    logger.debug("Extracting rows using element handles")
    data_list = []
    
    for row in rows:
        logger.debug('Processing row element')
        logger.debug(row)

        try:
            row_class = await row.get_attribute("class") or ""
        except Exception:
            logger.warning("Error reading row HTML or class", exc_info=True)
            row_class = ""
        
        if "day-breaker" in row_class or "no-event" in row_class:
            continue

        # Extract cell elements
        try:
            time_el = await row.select('.//td[contains(@class,"calendar__time")]')
            currency_el = await row.select('.//td[contains(@class,"calendar__currency")]')
            impact_el = await row.select('.//td[contains(@class,"calendar__impact")]')
            event_el = await row.select('.//td[contains(@class,"calendar__event")]')
            actual_el = await row.select('.//td[contains(@class,"calendar__actual")]')
            forecast_el = await row.select('.//td[contains(@class,"calendar__forecast")]')
            previous_el = await row.select('.//td[contains(@class,"calendar__previous")]')
        except Exception as e:
            logger.warning("Error reading row cells", exc_info=True)
            continue

        # Extract text from elements
        time_text = (await (time_el.get_text() if time_el else "")).strip() if time_el else ""
        currency_text = (await (currency_el.get_text() if currency_el else "")).strip() if currency_el else ""
        event_text = (await (event_el.get_text() if event_el else "")).strip() if event_el else ""
        actual_text = (await (actual_el.get_text() if actual_el else "")).strip() if actual_el else ""
        forecast_text = (await (forecast_el.get_text() if forecast_el else "")).strip() if forecast_el else ""
        previous_text = (await (previous_el.get_text() if previous_el else "")).strip() if previous_el else ""

        # Extract impact text (with special handling for span title)
        impact_text = ""
        try:
            impact_span = await (impact_el.select('.//span') if impact_el else None)
            if impact_span:
                impact_text = (await impact_span.get_attribute("title")) or ""
            if not impact_text and impact_el:
                impact_text = (await impact_el.get_text()).strip()
        except Exception as e:
            logger.warning("Error reading impact cell", exc_info=True)
            impact_text = (await (impact_el.get_text() if impact_el else "")).strip() if impact_el else ""

        # Parse time to datetime
        event_dt = parse_time_to_datetime(time_text, current_day)

        # Extract details if requested
        detail_str = ""
        if scrape_details:
            detail_str = await parse_event_details(
                page, row, event_dt, currency_text, event_text, 
                existing_df, mode="elements"
            )

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
    
    return data_list

def _detect_timezone_offset(header_time: str) -> timezone:
    """
    Detect timezone offset by comparing ForexFactory header time to system time.

    Args:
        header_time: Time string from FF header (e.g., "2:45pm")

    Returns:
        timezone object representing the offset
    """
    if not header_time:
        # Default to system local timezone
        local_offset = datetime.now().astimezone().utcoffset()
        return timezone(local_offset)

    # Parse the header time
    now = datetime.now()
    m = re.search(r'(\d{1,2}):(\d{2})\s*(am|pm)?', header_time.lower())
    if not m:
        local_offset = now.astimezone().utcoffset()
        return timezone(local_offset)

    hh = int(m.group(1))
    mm = int(m.group(2))
    ampm = m.group(3)

    if ampm:
        if ampm == 'pm' and hh < 12:
            hh += 12
        if ampm == 'am' and hh == 12:
            hh = 0

    # Create FF time for today
    ff_time = now.replace(hour=hh, minute=mm, second=0, microsecond=0)

    # Compare to system time (round to nearest 15 min to handle network delays)
    system_time = now.replace(second=0, microsecond=0)
    diff_minutes = (ff_time - system_time).total_seconds() / 60

    # Round to nearest 30 minutes for timezone offset
    offset_hours = round(diff_minutes / 30) * 0.5

    # FF time should be close to system time; if diff > 12 hours, FF is showing different day
    if abs(offset_hours) > 12:
        offset_hours = 0

    # Get system's local offset and adjust
    local_offset = now.astimezone().utcoffset()
    local_hours = local_offset.total_seconds() / 3600

    # The FF offset is: local_offset + difference
    # But since FF displays in user's set timezone, we assume it matches local
    return timezone(local_offset)


async def extract_via_javascript(rows_data, current_day: datetime, scrape_details: bool,
                                existing_df, page, header_time: str = "") -> list[CalendarEvent]:
    """
    Extract calendar data using JavaScript evaluation results.

    Args:
        rows_data: List of dictionaries from JavaScript evaluation
        current_day: Base date for the calendar day
        scrape_details: Whether to extract event details
        existing_df: DataFrame of existing data
        page: Browser page object
        header_time: Time from FF header for timezone detection

    Returns:
        List of CalendarEvent objects
    """
    logger.debug("Extracting rows using JavaScript data")
    events: list[CalendarEvent] = []

    # Detect timezone from header
    tz = _detect_timezone_offset(header_time)
    logger.debug(f"Using timezone: {tz}")

    def _convert_js_result(obj):
        """Convert nodriver's nested JS result format to flat dict."""
        if isinstance(obj, dict):
            if obj.get("type") == "object" and "value" in obj:
                # Convert [['key', {'type': 'string', 'value': 'val'}], ...] to {'key': 'val'}
                return {k: _convert_js_result(v) for k, v in obj["value"]}
            elif "type" in obj and "value" in obj:
                return obj["value"]
            else:
                return {k: _convert_js_result(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_convert_js_result(item) for item in obj]
        return obj

    last_time_text = ""  # Track last seen time for inherited times

    for idx, raw_rdict in enumerate(rows_data):
        logger.debug("JS mode row %d data: %s", idx, raw_rdict)

        # Convert nested format to flat dict
        rdict = _convert_js_result(raw_rdict)

        row_class = rdict.get("className", "") or ""
        if "day-breaker" in row_class or "no-event" in row_class:
            continue

        # Extract text fields from dictionary
        time_text = (rdict.get("time") or "").strip()
        currency_text = (rdict.get("currency") or "").strip()
        event_text = (rdict.get("event") or "").strip()
        actual_text = (rdict.get("actual") or "").strip()
        forecast_text = (rdict.get("forecast") or "").strip()
        previous_text = (rdict.get("previous") or "").strip()
        impact_text = (rdict.get("impact") or "").strip()

        # Skip rows without event name
        if not event_text:
            continue

        # Inherit time from previous event if empty or tentative
        if time_text and time_text.lower() != "tentative":
            last_time_text = time_text
        elif last_time_text:
            time_text = last_time_text

        # Parse time to datetime and make timezone-aware
        event_dt = parse_time_to_datetime(time_text, current_day)
        event_dt = event_dt.replace(tzinfo=tz)

        # Extract details if requested and available
        detail_str = ""
        if scrape_details and rdict.get("hasDetail", False):
            detail_str = await parse_event_details(
                page, idx, event_dt, currency_text, event_text,
                existing_df, mode="js"
            )

        # Create Currency object
        try:
            currency = Currency(symbol=currency_text) if currency_text else Currency(symbol="USD")
        except Exception:
            currency = Currency(symbol="USD")

        # Create CalendarEvent
        event = CalendarEvent(
            time=event_dt,
            currency=currency,
            impact=normalize_impact(impact_text),
            event=event_text,
            actual=actual_text or None,
            forecast=forecast_text or None,
            previous=previous_text or None,
            detail=detail_str or None
        )
        events.append(event)

    return events

# if __name__ == "__main__":
#     uc.loop().run_until_complete(scrape_range_pandas(from_date, to_date,
#     output_csv, tzname='US/Pacific', scrape_details=False))