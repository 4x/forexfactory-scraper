from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from forex_common import Currency
import re

class Impact(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    UNKNOWN = 4 # safer than 0

@dataclass
class CalendarEvent:
    time: datetime
    currency: Currency
    impact: Impact
    event: str 
    # actual: Optional[str] = None
    # forecast: Optional[str] = None
    # previous: Optional[str] = None
    # has_detail: bool = False
    # class_name: Optional[str] = None

    # But would consider adding some `from_row` method? Not now!
    # Carrying time over from previous event will be challenging.

def normalize_impact(text: str) -> Impact:
    text = (text or "").lower()
    if "high" in text:
        return Impact.HIGH
    elif "medium" in text:
        return Impact.MEDIUM
    elif "low" in text:
        return Impact.LOW
    return Impact.UNKNOWN

def parse_rows(rows, base_date: datetime) -> list[CalendarEvent]:
    events = []
    time = '0:00am'
    for row in rows:
        # row looks like: {"type":"object","value":[["currency",{"type":"string","value":"EUR"}], ...]}
        values = {k: v["value"] for k, v in row["value"]}

        # skip date-breakers with no event
        if not values.get("event"):
            continue

        t = values.get("time", '')
        if t and len(t) > 5: # e.g. '2:30pm'
            time = t
        # else use the time from previous event

        dtime = parse_time_to_datetime(time, base_date)

        events.append(CalendarEvent(
            time=dtime,
            currency=Currency(symbol=values.get("currency", "")),
            impact=normalize_impact(values.get("impact", "")),
            event=values.get("event", "")
            # actual=values.get("actual", ""),
            # forecast=values.get("forecast", ""),
            # previous=values.get("previous", ""),
            # has_detail=values.get("hasDetail", False),
            # class_name=values.get("className", ""),
        ))
    return events

def parse_time_to_datetime(time_text: str, base_date: datetime) -> datetime:
    """
    Shared time parsing logic for both extraction modes.
    Converts ForexFactory time text to datetime object.
    """
    event_dt = base_date
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
            hh = int(m.group(1))
            mm = int(m.group(2))
            ampm = m.group(3)
            if ampm:
                ampm = ampm.lower()
                if ampm == 'pm' and hh < 12:
                    hh += 12
                if ampm == 'am' and hh == 12:
                    hh = 0
            try:
                event_dt = event_dt.replace(hour=hh, minute=mm, second=0)
            except Exception:
                event_dt = event_dt.replace(hour=0, minute=0, second=0)
    
    return event_dt
