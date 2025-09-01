# src/forexfactory/scraper.py

import asyncio
import re
import logging
import pandas as pd
from datetime import datetime, timedelta
import nodriver as uc

from .csv_util import ensure_csv_header, read_existing_data, write_data_to_csv, merge_new_data

logger = logging.getLogger(__name__)


def detail_data_to_string(detail_data: dict) -> str:
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


# --------------------
# Main day parser
# --------------------
async def parse_calendar_day(page, the_date: datetime, scrape_details=False, existing_df=None) -> pd.DataFrame:
    """
    Scrape data for a single day (the_date) and return a DataFrame with columns:
      DateTime, Currency, Impact, Event, Actual, Forecast, Previous, Detail

    This function first tries to use nodriver's select/select_all. If those raise a DOM/Protocol
    exception (the '-32000' you saw), it falls back to running JS via page.evaluate(...) to
    collect the visible rows as a serializable list of dicts. Detail scraping (if requested)
    is handled via evaluate as well (click via JS, then extract the detail table).
    """
    date_str = the_date.strftime('%b%d.%Y').lower()
    url = f"https://www.forexfactory.com/calendar?day={date_str}"
    logger.info(f"Scraping URL: {url}")
    await page.get(url)

    # small delay to let JS start running (helps with race conditions)
    await asyncio.sleep(0.35)

    # ---- helper: robust wait for calendar table (tries select, then fallback evaluate)
    async def _wait_for_calendar_table_and_get_rows(page, url, max_attempts=3, xpath='//table[contains(@class,"calendar__table")]'):
        last_exc = None
        for attempt in range(1, max_attempts + 1):
            try:
                # try the normal fast path first
                if attempt > 1:
                    await asyncio.sleep(min(0.5 * (2 ** (attempt - 2)), 2.0))
                # this may raise ProtocolException if CDP dom query fails
                nodes = await page.select_all('//tr[contains(@class,"calendar__row")]')
                # normalize: some drivers return None
                if not nodes:
                    nodes = []
                return {"mode": "elements", "nodes": nodes}
            except Exception as exc:
                last_exc = exc
                msg = str(exc)
                logger.warning("Waiting for calendar table attempt %d/%d failed: %s", attempt, max_attempts, msg)
                # if it looks like the CDP DOM error, try a page reload/get once
                if ('DOM Error' in msg) or ('-32000' in msg) or ('Execution context' in msg) or ('context' in msg and 'destroyed' in msg):
                    try:
                        reload_fn = getattr(page, "reload", None)
                        if reload_fn:
                            res = reload_fn()
                            if asyncio.iscoroutine(res):
                                await res
                        else:
                            await page.get(url)
                        logger.info("Tried page.reload()/re-get after DOM error.")
                        # small pause after reload
                        await asyncio.sleep(0.5)
                    except Exception:
                        logger.debug("reload/get attempt failed", exc_info=True)
                # Next: try the JS fallback to directly pull row data from the page's runtime
                try:
                    js = r"""
                    (() => {
                        const rows = Array.from(document.querySelectorAll('tr.calendar__row'));
                        return rows.map(r => {
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
                    })();
                    """
                    rows_data = await page.evaluate(js)

                    logger.debug(rows_data)

                    # if JS returned an array, use it
                    if isinstance(rows_data, list) and len(rows_data) > 0:
                        return {"mode": "js", "rows_data": rows_data}
                except Exception as e2:
                    logger.debug("JS fallback evaluate failed on attempt %d: %s", attempt, e2, exc_info=True)

                # continue retry loop
                continue

        # if we got here, nothing succeeded. attempt to dump the page HTML to disk for debugging
        try:
            page_html = None
            for attr in ("get_content", "get_html", "content", "page_source", "source"):
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
            logger.debug("Failed to dump page HTML for debugging.", exc_info=True)

        raise last_exc or RuntimeError("Failed waiting for calendar table.")

    # call helper
    try:
        rows_result = await _wait_for_calendar_table_and_get_rows(page, url)
    except Exception as e:
        logger.warning(f"Extraction did not work for {the_date.date()}: {e}", exc_info=True)
        return pd.DataFrame(columns=["DateTime", "Currency", "Impact", "Event", "Actual", "Forecast", "Previous", "Detail"])

    # ----------------------------------------------------
    # Two modes now:
    #   - mode == "elements": nodes are nodriver elements (old path)
    #   - mode == "js": rows_data is a list of serializable dicts collected via evaluate
    # ----------------------------------------------------
    data_list = []
    current_day = the_date
    logger.debug("Found %d rows for %s using mode %s", len(rows_result.get("nodes", [])) if "nodes" in rows_result else len(rows_result.get("rows_data", [])), the_date.date(), rows_result["mode"])
    if rows_result["mode"] == "elements":
        rows = rows_result["nodes"]
        # iterate exactly like your original code but using safe helpers is recommended
        for row in rows:
            logger.debug('for row')

            try:
                row_class = await row.get_attribute("class") or ""
            except Exception:
                logger.warning("Error reading row HTML or class", exc_info=True)
                row_class = ""
            if "day-breaker" in row_class or "no-event" in row_class:
                continue

            # reuse your original selectors for cells (these should succeed since we have element handles)
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

            time_text = (await (time_el.get_text() if time_el else "")).strip() if time_el else ""
            currency_text = (await (currency_el.get_text() if currency_el else "")).strip() if currency_el else ""
            event_text = (await (event_el.get_text() if event_el else "")).strip() if event_el else ""
            actual_text = (await (actual_el.get_text() if actual_el else "")).strip() if actual_el else ""
            forecast_text = (await (forecast_el.get_text() if forecast_el else "")).strip() if forecast_el else ""
            previous_text = (await (previous_el.get_text() if previous_el else "")).strip() if previous_el else ""

            impact_text = ""
            try:
                impact_span = await (impact_el.select('.//span') if impact_el else None)
                if impact_span:
                    impact_text = (await impact_span.get_attribute("title")) or ""
                if not impact_text and impact_el:
                    impact_text = (await impact_el.get_text()).strip()
            except Exception:
                impact_text = (await (impact_el.get_text() if impact_el else "")).strip() if impact_el else ""

            # parse time -> event_dt (same logic as your original parser)
            event_dt = current_day
            time_lower = time_text.lower()
            if "day" in time_lower and "all day" in time_lower:
                event_dt = event_dt.replace(hour=0, minute=0, second=0)
            elif "day" in time_lower:
                event_dt = event_dt.replace(hour=23, minute=59, second=59)
            elif "data" in time_lower:
                event_dt = event_dt.replace(hour=0, minute=0, second=1)
            else:
                m = re.search(r'(\d{1,2}):(\d{2})\s*(am|pm)?', time_lower)
                if m:
                    hh = int(m.group(1)); mm = int(m.group(2)); ampm = m.group(3)
                    if ampm:
                        ampm = ampm.lower()
                        if ampm == 'pm' and hh < 12: hh += 12
                        if ampm == 'am' and hh == 12: hh = 0
                    try:
                        event_dt = event_dt.replace(hour=hh, minute=mm, second=0)
                    except Exception:
                        event_dt = event_dt.replace(hour=0, minute=0, second=0)

            # detail extraction using element handles (best-effort)
            detail_str = ""
            if scrape_details:
                try:
                    # check existing_df
                    if existing_df is not None:
                        matched = existing_df[
                            (existing_df["DateTime"] == event_dt.isoformat()) &
                            (existing_df["Currency"].str.strip() == currency_text) &
                            (existing_df["Event"].str.strip() == event_text)
                        ]
                        if not matched.empty:
                            existing_detail = str(matched.iloc[0]["Detail"]).strip() if pd.notnull(matched.iloc[0]["Detail"]) else ""
                            if existing_detail:
                                detail_str = existing_detail

                    if not detail_str:
                        open_link = await row.select('.//td[contains(@class,"calendar__detail")]/a')
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

                            # try to pull detail via element selector
                            try:
                                detail_element = await page.select('//tr[contains(@class,"calendar__details--detail")]', timeout=3)
                                detail_data = await parse_detail_table(detail_element)
                                detail_str = detail_data_to_string(detail_data)
                            except Exception:
                                logger.debug("Couldn't read detail element after click (elements path)", exc_info=True)

                            # try close
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
                except Exception:
                    logger.debug("Detail extraction error (elements path)", exc_info=True)

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

    else:
        # JS mode: rows_data is list of serializable dicts extracted via page.evaluate
        rows_data = rows_result["rows_data"]
        for idx, rdict in enumerate(rows_data):
            logger.debug("Row %d data: %s", idx, rdict)

            # html = await rdict.inner_html()
            logger.debug('else')
            logger.debug(rdict)

            row_class = rdict.get("className", "") or ""
            if "day-breaker" in row_class or "no-event" in row_class:
                continue

            time_text = (rdict.get("time") or "").strip()
            currency_text = (rdict.get("currency") or "").strip()
            event_text = (rdict.get("event") or "").strip()
            actual_text = (rdict.get("actual") or "").strip()
            forecast_text = (rdict.get("forecast") or "").strip()
            previous_text = (rdict.get("previous") or "").strip()
            impact_text = (rdict.get("impact") or "").strip()

            # parse time into event_dt (same rules)
            event_dt = current_day
            time_lower = time_text.lower()
            if "day" in time_lower and "all day" in time_lower:
                event_dt = event_dt.replace(hour=0, minute=0, second=0)
            elif "day" in time_lower:
                event_dt = event_dt.replace(hour=23, minute=59, second=59)
            elif "data" in time_lower:
                event_dt = event_dt.replace(hour=0, minute=0, second=1)
            else:
                m = re.search(r'(\d{1,2}):(\d{2})\s*(am|pm)?', time_lower)
                if m:
                    hh = int(m.group(1)); mm = int(m.group(2)); ampm = m.group(3)
                    if ampm:
                        ampm = ampm.lower()
                        if ampm == 'pm' and hh < 12: hh += 12
                        if ampm == 'am' and hh == 12: hh = 0
                    try:
                        event_dt = event_dt.replace(hour=hh, minute=mm, second=0)
                    except Exception:
                        event_dt = event_dt.replace(hour=0, minute=0, second=0)

            detail_str = ""
            if scrape_details and rdict.get("hasDetail", False):
                # check existing_df first
                try:
                    if existing_df is not None:
                        matched = existing_df[
                            (existing_df["DateTime"] == event_dt.isoformat()) &
                            (existing_df["Currency"].str.strip() == currency_text) &
                            (existing_df["Event"].str.strip() == event_text)
                        ]
                        if not matched.empty:
                            existing_detail = str(matched.iloc[0]["Detail"]).strip() if pd.notnull(matched.iloc[0]["Detail"]) else ""
                            if existing_detail:
                                detail_str = existing_detail
                    if not detail_str:
                        # click the detail link for this row index via JS, then extract detail via JS
                        # Inlining idx into JS string is safe here since idx is an integer
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
                                # wait for detail row to appear
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
                                    detail_str = detail_data_to_string(detail_data)
                                # try close
                                try:
                                    await page.evaluate("""() => { const c = document.querySelector('a[title="Close Detail"]'); if (c){ c.click(); return true } return false }""")
                                except Exception:
                                    pass
                        except Exception:
                            logger.debug("JS detail click/extract failed for idx %d", idx, exc_info=True)
                except Exception:
                    logger.debug("Error checking existing_df or extracting detail (js path)", exc_info=True)

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

    # Done
    return pd.DataFrame(data_list)

# --------------------
# Wrappers / orchestration
# --------------------
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
                    logger.info(merged_df.tail(new_rows).to_string(index=False))
                existing_df = merged_df
                total_new += max(0, new_rows)

                # Save updated data to CSV after processing the day's data.
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
                        logger.info(f"Chrome WebDriver {name} called successfully.")
                        return
                    except Exception as exc:
                        logger.warning(f"Attempt to call WebDriver.{name}() raised: {exc}")
                logger.warning("No supported shutdown method succeeded for WebDriver instance.")

            try:
                await _try_call_shutdown(browser)
            except Exception as e:
                logger.error(f"Error closing WebDriver: {e}")
            finally:
                browser = None

    # Final save (if needed)
    write_data_to_csv(existing_df, output_csv)
    logger.info(f"Done. Total new/updated rows: {total_new}")
