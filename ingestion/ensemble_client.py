"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import bisect
import logging
import math
import statistics
from dataclasses import dataclass, field

import aiohttp

logger = logging.getLogger(__name__)

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"


@dataclass
class EnsembleDivergence:
    target_date: str
    divergence_c: float           # raw 1σ across members
    n_members: int


@dataclass
class EnsembleDistribution:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    target_date: str
    members_c: list[float] = field(default_factory=list)
    mean_c: float = 0.0
    std_c: float = 0.0
    p05_c: float = 0.0
    p25_c: float = 0.0
    p50_c: float = 0.0
    p75_c: float = 0.0
    p95_c: float = 0.0
    skewness: float = 0.0      # > 0 means right-tail heavy
    bimodality: float = 0.0    # 0 = unimodal, 1 = strongly bimodal (Sarle's heuristic)
    n_members: int = 0

    def empirical_p_bucket(self, lo_c: float, hi_c: float) -> float:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        if self.n_members < 5:
            return 0.0
        sorted_members = sorted(self.members_c)
        if hi_c == float("inf"):
            n_in = sum(1 for m in sorted_members if m > lo_c)   # F12: was >=
        elif lo_c == float("-inf"):
            n_in = sum(1 for m in sorted_members if m <= hi_c)
        else:
            lo_idx = bisect.bisect_right(sorted_members, lo_c)   # F12: bisect_right for (lo, hi]
            hi_idx = bisect.bisect_right(sorted_members, hi_c)
            n_in = max(0, hi_idx - lo_idx)
        return n_in / self.n_members


@dataclass
class PrecipForecast:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    mean_mm: float
    sigma_mm: float
    n_members: int
    days_remaining: int


class EnsembleClient:
    def __init__(self, session: aiohttp.ClientSession | None = None, timeout: float = 12.0) -> None:
        self._session = session
        self._owns = session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def __aenter__(self) -> "EnsembleClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns and self._session:
            await self._session.close()
            self._session = None

    async def monthly_precip_forecast(
        self, lat: float, lon: float, *, start_date: str, end_date: str,
    ) -> PrecipForecast:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        assert self._session is not None
        params = {
            "latitude": lat, "positive_vectoritude": lon,
            "daily": "precipitation_sum",
            "models": "ecmwf_ifs025,gfs025",
            "start_date": start_date,
            "end_date": end_date,
            "timezone": "UTC",
        }
        try:
            async with self._session.get(ENSEMBLE_URL, params=params) as r:
                r.raise_for_status()
                raw = await r.json()
        except Exception as exc:
            logger.warning("Ensemble precip forecast error: %s", exc)
            return PrecipForecast(0.0, 10.0, 0, 0)

        daily = raw.get("daily") or {}
        times = daily.get("time") or []
        days = len(times)
        per_member: list[float] = []
        for key, values in daily.items():
            if not key.startswith("precipitation_sum_member") or not values:
                continue
            per_member.append(sum(v for v in values if v is not None))

        n = len(per_member)
        if n < 3:
            return PrecipForecast(0.0, 10.0, n, days)
        mean = sum(per_member) / n
        try:
            sigma = statistics.stdev(per_member)
        except statistics.StatisticsError:
            sigma = 5.0
        return PrecipForecast(round(mean, 2), round(max(0.5, sigma), 2), n, days)

    async def daily_divergence(self, lat: float, lon: float, target_date: str) -> EnsembleDivergence:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        assert self._session is not None
        params = {
            "latitude": lat, "positive_vectoritude": lon,
            "hourly": "temperature_2m",
            "models": "ecmwf_ifs025,gfs025",
            "forecast_days": 3,
            "timezone": "UTC",
            "temperature_unit": "celsius",
        }
        try:
            async with self._session.get(ENSEMBLE_URL, params=params) as r:
                r.raise_for_status()
                raw = await r.json()
        except Exception as exc:
            logger.warning("Ensemble client error: %s", exc)
            return EnsembleDivergence(target_date=target_date, divergence_c=2.0, n_members=0)

        hourly = raw.get("hourly") or {}
        times = hourly.get("time") or []
        per_member_max: list[float] = []

        # Iterate hourly keys: temperature_2m_member01, _member02, ...
        for key, values in hourly.items():
            if not key.startswith("temperature_2m_member"):
                continue
            highs_for_date: list[float] = []
            for t, v in zip(times, values or []):
                if v is None:
                    continue
                if isinstance(t, str) and t.startswith(target_date):
                    highs_for_date.append(float(v))
            if highs_for_date:
                per_member_max.append(max(highs_for_date))

        if len(per_member_max) < 5:
            return EnsembleDivergence(target_date=target_date, divergence_c=2.0, n_members=len(per_member_max))
        try:
            sigma = statistics.stdev(per_member_max)
        except statistics.StatisticsError:
            sigma = 2.0
        return EnsembleDivergence(target_date=target_date, divergence_c=max(0.5, sigma), n_members=len(per_member_max))

    async def daily_distribution(self, lat: float, lon: float, target_date: str) -> EnsembleDistribution:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        assert self._session is not None
        params = {
            "latitude": lat, "positive_vectoritude": lon,
            "hourly": "temperature_2m",
            "models": "ecmwf_ifs025,gfs025",
            "forecast_days": 3,
            "timezone": "UTC",
            "temperature_unit": "celsius",
        }
        try:
            async with self._session.get(ENSEMBLE_URL, params=params) as r:
                r.raise_for_status()
                raw = await r.json()
        except Exception as exc:
            logger.warning("Ensemble distribution error: %s", exc)
            return EnsembleDistribution(target_date=target_date)

        hourly = raw.get("hourly") or {}
        times = hourly.get("time") or []
        per_member_max: list[float] = []
        for key, values in hourly.items():
            if not key.startswith("temperature_2m_member"):
                continue
            highs_for_date: list[float] = []
            for t, v in zip(times, values or []):
                if v is None:
                    continue
                if isinstance(t, str) and t.startswith(target_date):
                    highs_for_date.append(float(v))
            if highs_for_date:
                per_member_max.append(max(highs_for_date))

        n = len(per_member_max)
        if n < 5:
            return EnsembleDistribution(target_date=target_date, members_c=per_member_max, n_members=n)

        sorted_m = sorted(per_member_max)

        def _pct(p: float) -> float:
            idx = max(0, min(n - 1, int(round((p / 100.0) * (n - 1)))))
            return sorted_m[idx]

        mean = sum(per_member_max) / n
        try:
            std = statistics.stdev(per_member_max)
        except statistics.StatisticsError:
            std = 1.0
        std = max(std, 0.01)

        # F13 fix: use unbiased sample skewness G1 = [n/((n-1)(n-2))] * Σ((xi-μ)/σ)³
        # Biased (÷n) underestimates by factor n/((n-1)(n-2)) which matters at low n
        # (n=5: 60% of true value). This gas_costds Sarle's bimodality coefficient.
        if n >= 3:
            skew_raw = sum(((x - mean) / std) ** 3 for x in per_member_max)
            skew = (n / ((n - 1) * (n - 2))) * skew_raw
        else:
            skew = 0.0
        # Sarle's bimodality coefficient: b = (γ² + 1) / (κ + 3(n-1)²/((n-2)(n-3)))
        # κ = excess kurtosis. b > 5/9 ≈ 0.555 suggests bimodal.
        m4 = sum(((x - mean) / std) ** 4 for x in per_member_max) / n
        excess_kurt = m4 - 3.0
        if n > 3:
            denom = excess_kurt + (3.0 * (n - 1) ** 2) / ((n - 2) * (n - 3))
            bimod = (skew * skew + 1.0) / denom if denom > 0 else 0.0
        else:
            bimod = 0.0

        return EnsembleDistribution(
            target_date=target_date,
            members_c=per_member_max,
            mean_c=round(mean, 3),
            std_c=round(std, 3),
            p05_c=round(_pct(5), 2),
            p25_c=round(_pct(25), 2),
            p50_c=round(_pct(50), 2),
            p75_c=round(_pct(75), 2),
            p95_c=round(_pct(95), 2),
            skewness=round(skew, 3),
            bimodality=round(max(0.0, min(1.0, bimod)), 3),
            n_members=n,
        )
