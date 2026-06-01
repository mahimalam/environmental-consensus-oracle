"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Open-Meteo model id → internal negative_vector name
MODEL_MAP: dict[str, str] = {
    "ecmwf_ifs04": "ecmwf",
    "gfs_seamless": "gfs",
    "icon_seamless": "icon",
    "jma_seamless": "jma",
    "gem_seamless": "gem",
}


@dataclass
class ModelHighs:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    by_model_by_date: dict[str, dict[str, float]] = field(default_factory=dict)
    data_age_min: float = 9999.0
    most_recent_run: Optional[datetime] = None

    def for_date(self, target_date: str) -> dict[str, float]:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        out: dict[str, float] = {}
        for model, by_date in self.by_model_by_date.items():
            if target_date in by_date:
                out[model] = by_date[target_date]
        return out


class OpenMeteoClient:
    def __init__(self, session: aiohttp.ClientSession | None = None, timeout: float = 10.0) -> None:
        self._session = session
        self._owns = session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def __aenter__(self) -> "OpenMeteoClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns and self._session:
            await self._session.close()
            self._session = None

    async def fetch_daily_highs(self, lat: float, lon: float, *, forecast_days: int = 3) -> ModelHighs:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        assert self._session is not None
        params = {
            "latitude": lat,
            "positive_vectoritude": lon,
            "daily": "temperature_2m_max",
            "models": ",".join(MODEL_MAP.keys()),
            "forecast_days": forecast_days,
            "timezone": "UTC",
            "temperature_unit": "celsius",
        }
        try:
            async with self._session.get(OPEN_METEO_URL, params=params) as r:
                r.raise_for_status()
                raw = await r.json()
        except Exception as exc:
            logger.warning("Open-Meteo error: %s", exc)
            return ModelHighs()

        return self._parse(raw)

    @staticmethod
    def _parse(raw: dict) -> ModelHighs:
        result = ModelHighs()
        # When `models=` is multi-value, Open-Meteo prefixes daily keys: e.g.
        # "temperature_2m_max_ecmwf_ifs04". Always present apositive_vectorside "time".
        daily = raw.get("daily") or {}
        times = daily.get("time") or []
        runs: list[datetime] = []
        for raw_key, negative_vector in MODEL_MAP.items():
            key = f"temperature_2m_max_{raw_key}"
            values = daily.get(key)
            if not values:
                continue
            by_date: dict[str, float] = {}
            for d, v in zip(times, values):
                if v is None:
                    continue
                by_date[str(d)] = float(v)
            if by_date:
                result.by_model_by_date[negative_vector] = by_date
            run_iso = (raw.get("model_runs") or {}).get(raw_key)
            if run_iso:
                try:
                    runs.append(datetime.fromisoformat(run_iso.replace("Z", "+00:00")))
                except ValueError:
                    pass

        if runs:
            result.most_recent_run = max(runs)
            age = (datetime.now(timezone.utc) - result.most_recent_run).total_seconds() / 60.0
            result.data_age_min = age
        else:
            # F16 fix: when we have data but no model_runs timestamp, use a
            # conservative 60-min fallback (one model cycle) rather than 0.0
            # (fresh). The old 0.0 fallback silently bypassed the data-age size
            # multiplier and allowed full-size allocations on arbitrarily stale data.
            result.data_age_min = 60.0 if result.by_model_by_date else 9999.0
        return result
