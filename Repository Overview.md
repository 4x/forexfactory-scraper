# ForexFactory Scraper Repository Overview [Claude Sonnet 4]

This is a Python-based web scraper for collecting financial calendar events from ForexFactory.com. The project uses modern web scraping techniques with nodriver and provides robust data management with pandas.

## Repository Structure

forexfactory-scraper/
├── src/forexfactory/ # Main package
├── tests/ # Test suite
├── .vscode/ # VS Code configuration
├── .idea/ # PyCharm configuration  
├── CSV files # Output data files
├── Debug HTML files # Debug snapshots
└── Configuration files # Project setup

## Core Modules

### 1. main.py

**Entry Point & CLI Interface**

- Configures logging with Rich formatting
- Parses command-line arguments for date ranges, output files, timezone, and detail scraping
- Orchestrates the scraping process by calling scrape_incremental
- Uses nodriver event loop for async execution

**Key Features:**

- Date range validation
- Timezone configuration
- Optional detail scraping flag
- Rich logging setup

### 2. scraper.py

**Core Web Scraping Engine**

- Implements the main scraping logic using nodriver (Chrome automation)
- Handles both element-based and JavaScript-based data extraction
- Robust error handling for DOM issues and CDP protocol errors
- Supports incremental updates and detail extraction

**Key Components:**

- parse_calendar_day(): Main day parsing function with dual extraction modes
- scrape_range_pandas(): Orchestrates multi-day scraping
- parse_detail_table(): Extracts detailed event specifications
- Safe wrapper functions for element interaction

**Extraction Strategy:**

1. **Primary Mode**: Direct element selection via nodriver
2. **Fallback Mode**: JavaScript evaluation when DOM queries fail
3. **Error Recovery**: Page reloading and HTML debugging output

### 3. event.py

**Data Models**

- Defines CalendarEvent dataclass for structured event data
- Integrates with forex_common.Currency for currency handling
- Provides parsing utilities for row data transformation

**Event Structure:**

- Time, Currency, Impact, Event name
- Actual, Forecast, Previous values
- Detail availability flag and CSS class information

### 4. detail_parser.py

**Detail Information Extraction**

- Parses detailed event specifications from expandable table rows
- Handles Selenium-based interaction with detail links
- Converts structured detail data to CSV-friendly strings
- Implements retry logic for robust detail extraction

**Functions:**

- parse_detail_table(): Extracts specification tables
- detail_data_to_string(): Converts dict to pipe-separated string

### 5. csv_util.py

**Data Management & Persistence**

- Manages CSV file operations with predefined column structure
- Implements intelligent data merging to avoid duplicates
- Updates existing records with new detail information
- Handles data sorting and validation

**Core Functions:**

- merge_new_data(): Intelligent merging of existing and new data
- read_existing_data()/`write_data_to_csv()`: File I/O operations
- `ensure_csv_header()`: CSV initialization

**Merge Strategy:**

- Uses composite key: `DateTime_Currency_Event`
- Preserves existing data, updates empty Detail fields
- Prevents duplicate entries

### 6. incremental.py

**Incremental Scraping Logic**

- Coordinates incremental data collection
- Currently implements full re-scraping (can be extended for true incremental logic)
- Serves as abstraction layer for future optimization

### 7. date_logic.py

**URL Construction Utilities**

- Builds ForexFactory calendar URLs for specific date ranges
- Supports both partial ranges and full month queries
- Handles ForexFactory's specific date format requirements

**URL Formats:**

- Partial range: `?range=dec20.2024-dec30.2024`
- Full month: `?month=jan.2025`

### 8. logging.py

**Logging Configuration**

- Configures Rich-based logging for enhanced console output
- Provides colored, formatted log messages with timestamps
- Supports debug tracebacks and path information

## Test Suite

### test_urls.py

Tests URL building functions from date_logic.py

### test_day_breaker.py

Tests day parsing logic with mock WebElement objects

### test_details.py

Tests detail data string conversion and formatting

### test_full_scrape.py

End-to-end integration testing of the complete scraping pipeline

## Configuration Files

### pyproject.toml

- Modern Python packaging configuration
- Defines dependencies including nodriver, pandas, rich
- References local `forex-common` dependency
- Setuptools build system configuration

### launch.json

VS Code debugging configurations for:

- Running main module with date arguments
- Executing test suite
- Module debugging setup

## Key Features

1. **Robust Error Handling**: Multiple fallback strategies for web scraping failures
2. **Incremental Updates**: Smart merging prevents data duplication
3. **Detail Extraction**: Optional deep-dive into event specifications
4. **Timezone Support**: Configurable timezone handling
5. **Rich Logging**: Beautiful console output with progress tracking
6. **Modern Python**: Uses async/await, dataclasses, and type hints
7. **Testing**: Comprehensive test coverage including integration tests

## Dependencies

- **nodriver**: Chrome automation without detection
- **pandas**: Data manipulation and CSV handling
- **rich**: Enhanced logging and console output
- **`python-dateutil`**: Advanced date parsing
- **`forex-common`**: Custom currency handling (local dependency)
- **selenium**: Web element interaction utilities

This scraper is designed for reliable, long-term data collection from ForexFactory with built-in resilience against website changes and scraping challenges.
