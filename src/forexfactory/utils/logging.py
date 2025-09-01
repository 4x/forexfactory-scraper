# src/forexfactory/utils/logging.py
import logging
from rich.logging import RichHandler

def configure_logging(level: int = logging.DEBUG) -> None:
    """Configure root logger with RichHandler."""
    logging.basicConfig(
        level=level,
        format="%(message)s",        # let RichHandler format
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_time=True, show_path=True)]
    )
