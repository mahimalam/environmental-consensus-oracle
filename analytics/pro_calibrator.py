"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any

from ... import db
from ...config import CONFIG, ENV
from ...notifications import telegram_bot
from .accuracy_tracker import backfill_actuals, update_local_bias

logger = logging.getLogger(__name__)


def _seconds_until_utc_hour(hour: int) -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


async def run_calibration_once() -> dict[str, Any]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(4)
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()

    # 1. Backfill actuals + per-model errors for yesterday
    from ...ingestion.weather.station_registry import STATIONS
    backfill_specs = [f"{s['station_id']}:{yesterday}" for s in STATIONS.values()]
    n_backfilled = await backfill_actuals(backfill_specs)

    # 2. Update local bias for stations with enough samples
    bias_updates: dict[str, Any] = {}
    for s in STATIONS.values():
        result = await update_local_bias(s["station_id"], min_samples=5)
        if result is not None:
            bias_updates[s["station_id"]] = {"bias_offset_c": result[0], "n_samples": result[1]}

    # 3. Recompute dispersion kappa per station from last 30d of model_accuracy
    kappa_updates: dict[str, float] = {}
    for s in STATIONS.values():
        sid = s["station_id"]
        with db.cursor() as cur:
            cur.execute(
                "SELECT error_c FROM model_accuracy WHERE city=? AND recorded_at >= ?",
                (sid, (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()),
            )
            errs = [float(r["error_c"]) for r in cur.fetchall() if r["error_c"] is not None]
        if len(errs) >= 8:
            actual_std = statistics.pstdev(errs)
            # F7 fix: kappa = actual_error_σ / expected_ensemble_σ.
            # Old code used denominator 1.0 (hardcoded °C), making kappa the
            # absolute error magnitude rather than the ratio realized/forecast.
            # e.g. ensemble_σ=2°C, error_σ=3°C → correct kappa=1.5, old=3.0.
            baseline_sigma = float(cfg.get("kappa_baseline_sigma_c", 2.0))
            kappa = max(1.0, min(2.5, actual_std / max(baseline_sigma, 0.1)))
            await db.upsert_station_kappa(sid, kappa)
            kappa_updates[sid] = kappa

    # 4. (Optional) Use Pro for narrative — kept simple in v1: write summary text
    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "backfilled_rows": n_backfilled,
        "bias_updates": bias_updates,
        "kappa_updates": kappa_updates,
        "skip_list": cfg.get("skip_list", []),
    }

    async with db.write_lock() as conn:
        conn.execute(
            """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]""",
            (
                summary["run_at"],
                json.dumps(bias_updates),
                json.dumps(summary["skip_list"]),
                None, None,
                f"Backfilled {n_backfilled} model_accuracy rows; "
                f"updated bias for {len(bias_updates)} stations, "
                f"kappa for {len(kappa_updates)} stations.",
            ),
        )

    if ENV.telegram_bot_unit:
        await telegram_bot.send_text(
            f"<b>E4 Pro nightly calibration</b>\n"
            f"Backfilled {n_backfilled} model_accuracy rows.\n"
            f"Updated bias for {len(bias_updates)} stations.\n"
            f"Updated kappa for {len(kappa_updates)} stations."
        )
    return summary


async def calibration_scheduler() -> None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(4)
    target_hour = int(cfg["pro_calibration_utc_hour"])
    while True:
        sleep_s = _seconds_until_utc_hour(target_hour)
        await asyncio.sleep(sleep_s)
        try:
            await run_calibration_once()
        except Exception:
            logger.exception("Pro calibration failed")
        # Avoid double-firing within the same UTC hour
        await asyncio.sleep(60)
