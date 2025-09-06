from dataclasses import dataclass
from typing import Optional
from forex_common import Currency

@dataclass
class CalendarEvent:
    time: str
    currency: Currency
    impact: str
    event: str
    actual: Optional[str] = None
    forecast: Optional[str] = None
    previous: Optional[str] = None
    has_detail: bool = False
    class_name: Optional[str] = None

def parse_rows(rows):
    events = []
    for row in rows:
        # row looks like: {"type":"object","value":[["currency",{"type":"string","value":"EUR"}], ...]}
        values = {k: v["value"] for k, v in row["value"]}

        # skip date-breakers with no event
        if not values.get("event"):
            continue

        events.append(CalendarEvent(
            time=values.get("time", ""),
            # currency=values.get("currency", "")),
            currency=Currency(symbol=values.get("currency", "")),
            impact=values.get("impact", ""),
            event=values.get("event", ""),
            actual=values.get("actual", ""),
            forecast=values.get("forecast", ""),
            previous=values.get("previous", ""),
            has_detail=values.get("hasDetail", False),
            class_name=values.get("className", ""),
        ))
    return events
