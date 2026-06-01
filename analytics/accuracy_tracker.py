"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from ... import db
from ...ingestion.weather.metar_client import MetarClient

logger = logging.getLogger(__name__)


async def backfill_actuals(target_dates: list[str]) -> int:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    n = 0
    async with MetarClient() as metar:
        for spec in target_dates:
            try:
                station_id, target = spec.split(":", 1)
            except ValueError:
                continue
            with db.cursor() as cur:
                cur.execute(
                    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]""",
                    (station_id, target),
                )
                rows = cur.fetchall()
            if not rows:
                continue
            from ...ingestion.weather.station_registry import STATIONS
            station = next((s for s in STATIONS.values() if s["station_id"] == station_id), None)
            if not station:
                continue
            summary = await metar.daily_summary(station_id, target, station["timezone"])
            actual = summary.observed_high_c
            if actual is None:
                continue
            for r in rows:
                for model in ("ecmwf", "gfs", "icon", "jma", "gem"):
                    fcst = r[f"{model}_high"]
                    if fcst is None:
                        continue
                    err = float(fcst) - float(actual)
                    async with db.write_lock() as conn:
                        conn.execute(
                            """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]""",
                            (
                                station_id, target, model,
                                float(fcst), float(actual), err,
                                datetime.now(timezone.utc).isoformat(),
                            ),
                        )
                    n += 1
    return n


async def update_local_bias(station_id: str, min_samples: int = 5) -> tuple[float, int] | None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    with db.cursor() as cur:
        cur.execute(
            """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]""",
            (station_id,),
        )
        rows = cur.fetchall()
    if len(rows) < min_samples:
        return None
    # err = forecast − actual; bias to apply to consensus = -mean(err)
    mean_err = sum(float(r["error_c"]) for r in rows) / len(rows)
    bias = -round(mean_err, 3)
    await db.upsert_station_bias(station_id, bias, len(rows), source="local")
    return bias, len(rows)
