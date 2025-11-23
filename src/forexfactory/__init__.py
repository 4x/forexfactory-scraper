"""
Scrape ForexFactory.com calendar events and return as pandas DataFrames.
"""
from .scraper import scrape_range_pandas

__all__ = ["scrape_range_pandas"]