# ForexFactory Scraper

Scrape economic calendar events from ForexFactory.com and return as a pandas DataFrame.

## Installation

### As an editable package (recommended)

```bash
# Clone the repository
git clone https://github.com/4x/forexfactory-scraper.git
cd forexfactory-scraper

# Install with uv (recommended)
uv pip install -e .

# Or with pip
pip install -e .
```

### Dependencies

- Python 3.9+
- Chrome browser (for nodriver)
- [forex-common](../forex_common) package (local dependency)

## Usage from Python

### Basic usage

```python
import asyncio
from datetime import datetime
from forexfactory import scrape_range_pandas

async def main():
    # Scrape events for a date range
    df = await scrape_range_pandas(
        from_date=datetime(2025, 11, 24),
        to_date=datetime(2025, 11, 30),
        scrape_details=False
    )

    print(f"Found {len(df)} events")
    print(df.head())

asyncio.run(main())
```

### With CSV output

```python
df = await scrape_range_pandas(
    from_date=datetime(2025, 11, 24),
    to_date=datetime(2025, 11, 30),
    output_csv="calendar.csv",  # Optional: save to CSV
    scrape_details=False
)
```

### DataFrame structure

The returned DataFrame has these columns:

| Column | Type | Description |
|--------|------|-------------|
| DateTime | str | ISO format with timezone (e.g., `2025-11-24T08:30:00-05:00`) |
| Currency | str | Currency code (e.g., `USD`, `EUR`) |
| Impact | str | `HIGH`, `MEDIUM`, `LOW`, `HOLIDAY`, or empty |
| Event | str | Event name |
| Actual | str | Actual value (empty for future events) |
| Forecast | str | Forecasted value |
| Previous | str | Previous value |
| Detail | str | Additional details (if `scrape_details=True`) |

### Using CalendarEvent objects

```python
from forexfactory.event import CalendarEvent, Impact, normalize_impact
from forex_common import Currency

# The scraper internally creates CalendarEvent objects:
# CalendarEvent(
#     time=datetime(2025, 11, 24, 8, 30, tzinfo=...),
#     currency=Currency(symbol="USD"),
#     impact=Impact.HIGH,
#     event="Non-Farm Payrolls",
#     actual=None,
#     forecast="200K",
#     previous="150K",
#     detail=None
# )
```

### Filter by impact

```python
# Get only high-impact events
high_impact = df[df['Impact'] == 'HIGH']

# Get events for specific currency
usd_events = df[df['Currency'] == 'USD']
```

### Working with timezone-aware DateTimes

```python
import pandas as pd

# Parse DateTime column to datetime objects
df['DateTime'] = pd.to_datetime(df['DateTime'])

# Filter by date
today_events = df[df['DateTime'].dt.date == datetime.today().date()]

# Convert to different timezone
df['DateTime'] = df['DateTime'].dt.tz_convert('UTC')
```

## Command Line Usage

```bash
python -m forexfactory.main --start 2025-11-24 --end 2025-11-30 --csv events.csv
```

Options:
- `--start`: Start date (YYYY-MM-DD) **required**
- `--end`: End date (YYYY-MM-DD) **required**
- `--csv`: Output CSV file (default: forex_factory_cache.csv)
- `--details`: Include event details

## Running Tests

```bash
# Unit tests (no network required)
uv run python -m pytest tests/test_event.py tests/test_urls.py -v

# Integration tests (requires Chrome and internet)
uv run python -m pytest tests/integration/ -v

# All tests
uv run python -m pytest tests/ -v
```

## Project Structure

```
src/forexfactory/
├── __init__.py          # Package exports
├── scraper.py           # Core scraping logic
├── event.py             # CalendarEvent dataclass
├── date_logic.py        # URL building utilities
├── main.py              # CLI entry point
└── utils/
    ├── csv_util.py      # CSV operations
    └── logging.py       # Logging configuration
```

## Notes

- **Timezone handling**: Times are returned in local timezone (detected from system). The DateTime string includes the UTC offset (e.g., `-05:00` for EST).
- **Rate limiting**: The scraper includes delays between requests to avoid being blocked.
- **Chrome requirement**: Uses [nodriver](https://github.com/nicegui/nodriver) for browser automation, which requires Chrome installed.
- **Future dates**: ForexFactory shows scheduled events for future dates with forecasts but no actual values.

## License

MIT
