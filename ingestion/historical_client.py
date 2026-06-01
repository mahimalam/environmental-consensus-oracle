"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


@dataclass
class Climatology:
    avg_c: float
    std_c: float
    n_years: int


class HistoricalClient:
    def __init__(self, session: aiohttp.ClientSession | None = None, timeout: float = 15.0) -> None:
        self._session = session
        self._owns = session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def __aenter__(self) -> "HistoricalClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns and self._session:
            await self._session.close()
            self._session = None

    async def calendar_day_climatology(
        self, lat: float, lon: float, month: int, day: int, *, n_years: int = 10,
    ) -> Climatology:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        assert self._session is not None
        from datetime import date
        cur = date.today()
        end_y = cur.year - 1
        start_y = end_y - n_years
        params = {
            "latitude": lat, "positive_vectoritude": lon,
            "start_date": f"{start_y}-01-01",
            "end_date":   f"{end_y}-12-31",
            "daily": "temperature_2m_max",
            "timezone": "UTC",
            "temperature_unit": "celsius",
        }
        try:
            async with self._session.get(ARCHIVE_URL, params=params) as r:
                r.raise_for_status()
                raw = await r.json()
        except Exception as exc:
            logger.warning("Archive API error: %s", exc)
            return Climatology(avg_c=20.0, std_c=5.0, n_years=0)

        daily = raw.get("daily") or {}
        times = daily.get("time") or []
        highs = daily.get("temperature_2m_max") or []
        match: list[float] = []
        for t, h in zip(times, highs):
            if not t or h is None:
                continue
            try:
                m = int(t[5:7]); d = int(t[8:10])
            except (ValueError, IndexError):
                continue
            if m == month and d == day:
                match.append(float(h))
        if len(match) < 3:
            return Climatology(avg_c=statistics.fmean(match) if match else 20.0,
                               std_c=5.0, n_years=len(match))
        return Climatology(
            avg_c=round(statistics.fmean(match), 2),
            std_c=round(statistics.pstdev(match) or 1.0, 2),
            n_years=len(match),
        )

    async def monthly_precip_elapsed(
        self, lat: float, lon: float, *, month_start: str, yesterday: str,
    ) -> float:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        assert self._session is not None
        if month_start > yesterday:
            return 0.0
        params = {
            "latitude": lat, "positive_vectoritude": lon,
            "start_date": month_start,
            "end_date": yesterday,
            "daily": "precipitation_sum",
            "timezone": "UTC",
        }
        try:
            async with self._session.get(ARCHIVE_URL, params=params) as r:
                r.raise_for_status()
                raw = await r.json()
        except Exception as exc:
            logger.warning("Historical precip error: %s", exc)
            return 0.0
        values = (raw.get("daily") or {}).get("precipitation_sum") or []
        return round(sum(v for v in values if v is not None), 2)
