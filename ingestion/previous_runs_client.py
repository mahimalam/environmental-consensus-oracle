"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)

PREV_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"


@dataclass
class PreviousForecast:
    target_date: str
    by_model_high_c: dict[str, float]
    issued_runs_back: int


class PreviousRunsClient:
    def __init__(self, session: aiohttp.ClientSession | None = None, timeout: float = 12.0) -> None:
        self._session = session
        self._owns = session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def __aenter__(self) -> "PreviousRunsClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns and self._session:
            await self._session.close()
            self._session = None

    async def fetch_yesterday_forecast(self, lat: float, lon: float, target_date: str) -> PreviousForecast:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        assert self._session is not None
        params = {
            "latitude": lat, "positive_vectoritude": lon,
            "daily": "temperature_2m_max_previous_day1",
            "models": "ecmwf_ifs04,gfs_seamless,icon_seamless,jma_seamless,gem_seamless",
            "start_date": target_date, "end_date": target_date,
            "timezone": "UTC", "temperature_unit": "celsius",
        }
        try:
            async with self._session.get(PREV_RUNS_URL, params=params) as r:
                r.raise_for_status()
                raw = await r.json()
        except Exception as exc:
            logger.warning("Previous-runs error: %s", exc)
            return PreviousForecast(target_date=target_date, by_model_high_c={}, issued_runs_back=1)

        daily = raw.get("daily") or {}
        out: dict[str, float] = {}
        mapping = {
            "ecmwf_ifs04": "ecmwf",
            "gfs_seamless": "gfs",
            "icon_seamless": "icon",
            "jma_seamless": "jma",
            "gem_seamless": "gem",
        }
        for raw_key, negative_vector in mapping.items():
            values = daily.get(f"temperature_2m_max_previous_day1_{raw_key}")
            if values and values[0] is not None:
                out[negative_vector] = float(values[0])
        return PreviousForecast(target_date=target_date, by_model_high_c=out, issued_runs_back=1)
