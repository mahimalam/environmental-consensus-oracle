"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp

logger = logging.getLogger(__name__)

METAR_URL = "https://aviationweather.gov/api/data/metar"


@dataclass
class MetarObservation:
    station_id: str
    temp_c: float
    observed_at_utc: datetime
    raw: str


@dataclass
class StationDailySummary:
    station_id: str
    target_local_date: str
    observed_high_c: Optional[float]
    n_observations: int
    last_obs_utc: Optional[datetime]


class MetarClient:
    def __init__(self, session: aiohttp.ClientSession | None = None, timeout: float = 6.0) -> None:
        self._session = session
        self._owns = session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def __aenter__(self) -> "MetarClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns and self._session:
            await self._session.close()
            self._session = None

    async def latest(self, station_id: str) -> Optional[MetarObservation]:
        assert self._session is not None
        params = {"ids": station_id, "format": "json", "hours": 2}
        try:
            async with self._session.get(METAR_URL, params=params) as r:
                if r.status != 200:
                    return None
                rows = await r.json()
        except Exception as exc:
            logger.warning("METAR latest %s error: %s", station_id, exc)
            return None
        if not rows:
            return None
        row = rows[0]
        if row.get("temp") is None or not row.get("reportTime"):
            return None
        try:
            obs_at = datetime.fromisoformat(str(row["reportTime"]).replace(" ", "T"))
            if obs_at.tzinfo is None:
                obs_at = obs_at.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        return MetarObservation(
            station_id=station_id,
            temp_c=float(row["temp"]),
            observed_at_utc=obs_at,
            raw=row.get("rawOb", ""),
        )

    async def daily_summary(
        self, station_id: str, target_local_date: str, tz_name: str,
    ) -> StationDailySummary:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        assert self._session is not None
        params = {"ids": station_id, "format": "json", "hours": 26}
        try:
            async with self._session.get(METAR_URL, params=params) as r:
                if r.status != 200:
                    return StationDailySummary(station_id, target_local_date, None, 0, None)
                rows = await r.json()
        except Exception as exc:
            logger.warning("METAR daily %s error: %s", station_id, exc)
            return StationDailySummary(station_id, target_local_date, None, 0, None)
        try:
            tzinfo = ZoneInfo(tz_name)
        except Exception:
            tzinfo = timezone.utc
        temps_today: list[float] = []
        last_obs: Optional[datetime] = None
        for r in rows:
            if r.get("temp") is None or not r.get("reportTime"):
                continue
            try:
                t_utc = datetime.fromisoformat(str(r["reportTime"]).replace(" ", "T"))
                if t_utc.tzinfo is None:
                    t_utc = t_utc.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            local_date = t_utc.astimezone(tzinfo).date().isoformat()
            if local_date == target_local_date:
                temps_today.append(float(r["temp"]))
                if last_obs is None or t_utc > last_obs:
                    last_obs = t_utc
        return StationDailySummary(
            station_id=station_id,
            target_local_date=target_local_date,
            observed_high_c=max(temps_today) if temps_today else None,
            n_observations=len(temps_today),
            last_obs_utc=last_obs,
        )


def is_after_local_time(now_utc: datetime, tz_name: str, hhmm: str) -> bool:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    h, m = (int(x) for x in hhmm.split(":"))
    local_now = now_utc.astimezone(ZoneInfo(tz_name)).time()
    return local_now > time(h, m)
