"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date as _date
from typing import Iterable, Optional

from ...ingestion.gamma_client import GammaClient, GammaEvent, GammaEventNode
from ...ingestion.weather.station_registry import STATIONS, find_by_alias

logger = logging.getLogger(__name__)

KIND_TEMPERATURE = "temperature"
KIND_PRECIPITATION = "precipitation"

WEATHER_KEYWORDS = (
    "temperature", "high temperature", "°c", "°f", "celsius", "fahrenheit",
    "degrees fahrenheit", "degrees celsius",
    "precipitation", "rainfall",
)

UNIT_PATTERNS_F = re.compile(r"(°\s*F|degrees?\s+fahrenheit|\bfahrenheit\b)", re.I)
UNIT_PATTERNS_C = re.compile(r"(°\s*C|degrees?\s+celsius|\bcelsius\b)", re.I)

# "75-76°F" / "75 to 76 F" / "75°F to 76°F" / "≥75°F" / "above 75" / "below 60"
BUCKET_RANGE_RE = re.compile(
    r"(?P<lo>\-?\d+(?:\.\d+)?)\s*°?\s*[CFcf]?\s*(?:to|–|—|-|through)\s*(?P<hi>\-?\d+(?:\.\d+)?)\s*°?\s*[CFcf]?",
    re.I,
)
BUCKET_ABOVE_RE = re.compile(
    r"(?:above|over|≥|>=|or higher|or more|or above)\s*(?P<lo>\-?\d+(?:\.\d+)?)", re.I,
)
BUCKET_BELOW_RE = re.compile(
    r"(?:below|under|≤|<=|or lower|or less|or below)\s*(?P<hi>\-?\d+(?:\.\d+)?)", re.I,
)


def is_weather_event(event: GammaEvent) -> bool:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    blob = f"{event.title} {event.description}".lower()
    return any(k in blob for k in WEATHER_KEYWORDS)


def detect_unit(*texts: str) -> Optional[str]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    blob = "\n".join(t for t in texts if t)
    has_f = bool(UNIT_PATTERNS_F.search(blob))
    has_c = bool(UNIT_PATTERNS_C.search(blob))
    if has_f and not has_c:
        return "F"
    if has_c and not has_f:
        return "C"
    return None


def f_to_c(f: float) -> float:
    return (f - 32.0) * 5.0 / 9.0


def parse_bucket_label(label: str, unit: str) -> Optional[tuple[float, float]]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if not label:
        return None
    label = label.strip()
    m = BUCKET_RANGE_RE.search(label)
    if m:
        lo, hi = float(m.group("lo")), float(m.group("hi"))
        if unit == "F":
            lo, hi = f_to_c(lo), f_to_c(hi)
        if lo > hi:
            lo, hi = hi, lo
        return (lo, hi)
    m = BUCKET_ABOVE_RE.search(label)
    if m:
        lo = float(m.group("lo"))
        if unit == "F":
            lo = f_to_c(lo)
        return (lo, float("inf"))
    m = BUCKET_BELOW_RE.search(label)
    if m:
        hi = float(m.group("hi"))
        if unit == "F":
            hi = f_to_c(hi)
        return (float("-inf"), hi)
    return None


_IN2MM = 25.4  # inches → mm

# Precipitation bucket regexes (match actual public_sentiment_node question text)
_PRECIP_BETWEEN_IN = re.compile(
    r'allocateween\s+(?P<lo>\d+\.?\d*)\s+and\s+(?P<hi>\d+\.?\d*)\s+inch', re.I
)
_PRECIP_BETWEEN_MM = re.compile(
    r'allocateween\s+(?P<lo>\d+\.?\d*)\s*[-–—]\s*(?P<hi>\d+\.?\d*)\s*mm', re.I
)
_PRECIP_LT_IN = re.compile(r'less\s+than\s+(?P<hi>\d+\.?\d*)\s+inch', re.I)
_PRECIP_LT_MM = re.compile(r'less\s+than\s+(?P<hi>\d+\.?\d*)\s*mm', re.I)
_PRECIP_GT_IN = re.compile(r'more\s+than\s+(?P<lo>\d+\.?\d*)\s+inch', re.I)
_PRECIP_GT_MM = re.compile(
    r'(?:more\s+than\s+(?P<lo1>\d+\.?\d*)\s*mm|(?P<lo2>\d+\.?\d*)\s*mm\s+or\s+more)', re.I
)

# Known city slugs for precipitation-in-{city}-in-{month} pattern
_PRECIP_CITY_SLUGS = [
    "nyc", "new-york", "london", "hong-kong", "seoul", "seattle",
    "tokyo", "singapore", "dubai", "paris", "berlin", "chicago",
    "miami", "los-angeles", "sydney", "toronto", "amsterdam", "madrid",
    "rome", "beijing", "shanghai", "mumbai", "delhi", "bangkok",
    "kuala-lumpur", "jakarta", "manila", "cairo", "nairobi",
    "johannesburg", "casablanca", "lagos", "accra",
    "sao-paulo", "buenos-aires", "bogota", "lima", "santiago", "mexico-city",
]


def parse_precip_bucket_label(label: str) -> Optional[tuple[float, float]]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if not label:
        return None
    m = _PRECIP_BETWEEN_IN.search(label)
    if m:
        lo, hi = float(m.group("lo")) * _IN2MM, float(m.group("hi")) * _IN2MM
        return (min(lo, hi), max(lo, hi))
    m = _PRECIP_BETWEEN_MM.search(label)
    if m:
        lo, hi = float(m.group("lo")), float(m.group("hi"))
        return (min(lo, hi), max(lo, hi))
    m = _PRECIP_LT_IN.search(label)
    if m:
        return (0.0, float(m.group("hi")) * _IN2MM)
    m = _PRECIP_LT_MM.search(label)
    if m:
        return (0.0, float(m.group("hi")))
    m = _PRECIP_GT_IN.search(label)
    if m:
        return (float(m.group("lo")) * _IN2MM, float("inf"))
    m = _PRECIP_GT_MM.search(label)
    if m:
        lo = m.group("lo1") or m.group("lo2")
        return (float(lo), float("inf"))
    return None


@dataclass
class WeatherBucket:
    event_node: GammaEventNode
    label: str
    lo_c: float           # °C for temp; mm for precip
    hi_c: float           # °C for temp; mm for precip
    yes_unit_id: str
    yes_upper_bound: Optional[float]
    kind: str = KIND_TEMPERATURE


@dataclass
class WeatherEventScan:
    event: GammaEvent
    city: str
    station: dict
    unit: str                     # 'C' or 'F' for temp; 'mm' or 'inch' for precip
    target_date_iso: str
    buckets: list[WeatherBucket]
    kind: str = KIND_TEMPERATURE


async def find_weather_events(gamma: GammaClient) -> list[GammaEvent]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    events = await gamma.list_events(active=True, closed=False, limit=300)
    return [e for e in events if is_weather_event(e)]


async def find_precip_events(gamma: GammaClient) -> list[GammaEvent]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    import calendar
    today = _date.today()
    months = []
    for delta in range(3):
        m = (today.month - 1 + delta) % 12 + 1
        y = today.year + ((today.month - 1 + delta) // 12)
        months.append(calendar.month_name[m].lower())  # "may", "june", ...

    events: list[GammaEvent] = []
    seen: set[str] = set()
    for city in _PRECIP_CITY_SLUGS:
        for month in months:
            slug = f"precipitation-in-{city}-in-{month}"
            try:
                ev = await gamma.get_event_by_slug(slug)
            except Exception:
                continue
            if ev and ev.id not in seen and not ev.raw.get("closed"):
                seen.add(ev.id)
                events.append(ev)
    logger.info("E4 precip discovery: %d active events found", len(events))
    return events


def scan_precip_event(event: GammaEvent) -> Optional[WeatherEventScan]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if not event.event_nodes:
        return None

    station = find_by_alias(event.title) or find_by_alias(event.description)
    if not station:
        logger.info("E4 precip skip [%s] — no station match", event.title[:60])
        return None

    buckets: list[WeatherBucket] = []
    for m in event.event_nodes:
        for label_source in (m.question, *m.outcomes):
            parsed = parse_precip_bucket_label(label_source)
            if parsed:
                lo_mm, hi_mm = parsed
                if not m.yes_unit_id:
                    break
                yes_upper_bound = m.outcome_metrics[0] if m.outcome_metrics else None
                buckets.append(WeatherBucket(
                    event_node=m, label=label_source.strip(),
                    lo_c=lo_mm, hi_c=hi_mm,
                    yes_unit_id=m.yes_unit_id,
                    yes_upper_bound=yes_upper_bound,
                    kind=KIND_PRECIPITATION,
                ))
                break

    if len(buckets) < 2:
        logger.info("E4 precip skip [%s] — only %d parseable buckets", event.title[:60], len(buckets))
        return None

    return WeatherEventScan(
        event=event, city=station["city"], station=station, unit="mm",
        target_date_iso=event.end_date_iso or "",
        buckets=buckets,
        kind=KIND_PRECIPITATION,
    )


def scan_event(event: GammaEvent) -> Optional[WeatherEventScan]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if not event.event_nodes:
        logger.info("E4 scan_event skip [%s] — no event_nodes", event.title[:60])
        return None

    station = find_by_alias(event.title) or find_by_alias(event.description)
    if not station:
        logger.info("E4 scan_event skip [%s] — no station match", event.title[:60])
        return None

    unit = detect_unit(event.title, event.description, *(m.question for m in event.event_nodes))
    if unit is None:
        unit = station.get("resolution_unit_default")
    if unit not in ("C", "F"):
        logger.info("E4 scan_event skip [%s] — ambiguous unit (got %r)", event.title[:60], unit)
        return None

    buckets: list[WeatherBucket] = []
    for m in event.event_nodes:
        # outcomes is typically ["Yes","No"]; the bucket label is the question itself
        # for one-bucket-per-event_node events. For multi-outcome we'd parse outcomes[i].
        for label_source in (m.question, *m.outcomes):
            parsed = parse_bucket_label(label_source, unit)
            if parsed:
                lo_c, hi_c = parsed
                if not m.yes_unit_id:
                    break
                yes_upper_bound = m.outcome_metrics[0] if m.outcome_metrics else None
                buckets.append(WeatherBucket(
                    event_node=m, label=label_source.strip(),
                    lo_c=lo_c, hi_c=hi_c,
                    yes_unit_id=m.yes_unit_id,
                    yes_upper_bound=yes_upper_bound,
                ))
                break

    if len(buckets) < 2:
        logger.info("E4 scan_event skip [%s] — only %d parseable buckets (need ≥2)", event.title[:60], len(buckets))
        return None

    return WeatherEventScan(
        event=event, city=station["city"], station=station, unit=unit,
        target_date_iso=event.end_date_iso or "",
        buckets=buckets,
    )
