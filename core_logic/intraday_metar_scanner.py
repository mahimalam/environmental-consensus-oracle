"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo

from ...common import net_circuit
from ...common.gas_costs import net_edge_pct
from ...common.net_errors import Backoff, is_network_error
from ...config import CONFIG
from ...ingestion.network_client import NetworkClient
from ...ingestion.gamma_client import GammaClient
from ...ingestion.weather.metar_client import MetarClient
from ..opportunity import Leg, Opportunity
from .event_node_scanner import KIND_TEMPERATURE, is_weather_event, scan_event
from .probability_estimator import enforce_live_min_qty, kelly_fraction, kelly_tier_fraction, p_bucket

logger = logging.getLogger(__name__)


@dataclass
class _IntradayState:
    seen_keys: deque = field(default_factory=lambda: deque(maxlen=2000))


_state = _IntradayState()


def _past_peak_window(now_utc: datetime, tz_name: str, peak_end_hhmm: str) -> bool:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    try:
        local_now = now_utc.astimezone(ZoneInfo(tz_name)).time()
        h, m = (int(x) for x in peak_end_hhmm.split(":"))
        return local_now > time(h, m)
    except Exception:
        return False


async def intraday_metar_scan_once(
    *,
    gamma: GammaClient,
    network: NetworkClient,
    metar: MetarClient,
    emit: Callable[[Opportunity], Awaitable[None]],
    available_resource_provider: Callable[[], float],
) -> int:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if net_circuit.is_open():
        return 0

    cfg = CONFIG.engine(4)
    intraday_cfg = cfg.get("intraday_metar", {}) or {}
    if not intraday_cfg.get("enabled", True):
        return 0

    edge_min = float(intraday_cfg.get("min_edge", 0.05))
    max_per_execution = float(intraday_cfg.get("max_per_execution_base_units", 1.5))
    post_peak_sigma_c = float(intraday_cfg.get("post_peak_sigma_c", 0.5))
    min_upper_bound = float(intraday_cfg.get("min_upper_bound", 0.03))
    max_upper_bound = float(intraday_cfg.get("max_upper_bound", 0.70))
    provider_wait_sec = float(intraday_cfg.get("provider_wait_sec", 30.0))
    min_payload_base_units = float(CONFIG.globals.get("min_payload_base_units_live", 1.05))
    from ...config import ENV

    try:
        events = await gamma.list_events(active=True, closed=False, limit=300)
    except Exception as exc:
        if is_network_error(exc):
            net_circuit.record_failure()
            raise
        logger.debug("E4 intraday: gamma list_events failed: %s", exc)
        return 0

    weather_events = [e for e in events if is_weather_event(e)]
    if not weather_events:
        return 0

    n_emitted = 0
    seen_cities: set[tuple[str, str]] = set()
    resource = available_resource_provider()
    now_utc = datetime.now(timezone.utc)

    for event in weather_events:
        scan = scan_event(event)
        if not scan or scan.kind != KIND_TEMPERATURE:
            continue
        city_key = (scan.city, event.id)
        if city_key in seen_cities:
            continue
        seen_cities.add(city_key)

        station = scan.station
        target = scan.target_date_iso[:10] if scan.target_date_iso else None
        if not target:
            continue

        peak_end = station["peak_window_local"][1]
        if not _past_peak_window(now_utc, station["timezone"], peak_end):
            continue

        try:
            summary = await metar.daily_summary(station["station_id"], target, station["timezone"])
        except Exception as exc:
            if is_network_error(exc):
                raise
            continue
        obs = summary.observed_high_c
        if obs is None:
            continue

        unit_ids = [b.yes_unit_id for b in scan.buckets if b.yes_unit_id]
        if not unit_ids:
            continue
        try:
            books = await network.get_books(unit_ids)
        except Exception as exc:
            if is_network_error(exc):
                raise
            continue
        book_map = {bk.unit_id: bk.best_upper_bound for bk in books}

        try:
            end_dt = datetime.fromisoformat(scan.target_date_iso.replace("Z", "+00:00"))
            expected_unlock = end_dt + timedelta(
                hours=float(station.get("typical_resolution_lag_hours", 4.0)),
            )
        except Exception:
            expected_unlock = now_utc + timedelta(hours=24)

        for b in scan.buckets:
            upper_bound = book_map.get(b.yes_unit_id)
            if upper_bound is None or upper_bound < min_upper_bound or upper_bound > max_upper_bound:
                continue
            # Skip buckets already exceeded by observation (P → 0).
            if obs >= b.hi_c:
                continue

            # Tight post-peak Gaussian on the observation. For a bucket
            # containing obs with σ=0.5, P ≈ 0.85-0.95.
            p = p_bucket(obs, post_peak_sigma_c, b.lo_c, b.hi_c)
            edge = net_edge_pct(p, upper_bound) / 100.0
            if edge < edge_min:
                continue

            key = f"intraday:{scan.city}:{event.id}:{b.event_node.id}:{round(obs, 1)}"
            if key in _state.seen_keys:
                continue
            _state.seen_keys.append(key)

            kelly_tier = kelly_tier_fraction(edge)
            kf = kelly_fraction(p, upper_bound)
            raw = kelly_tier * kf * resource
            basis = min(raw, max_per_execution)
            if basis < CONFIG.globals["min_execution_base_units"]:
                continue
            qty = int(basis / upper_bound)
            qty = enforce_live_min_qty(qty, upper_bound, bool(ENV.paper_execution), min_payload_base_units)
            if qty < 1:
                continue
            actual_basis = round(qty * upper_bound, 4)

            leg = Leg(
                unit_id=b.yes_unit_id, side="YES",
                metric=float(upper_bound), qty=qty,
                event_node_id=b.event_node.id, event_node_title=b.event_node.question,
            )
            opp = Opportunity(
                engine="E4", kind="WEATHER_INTRADAY_LOCK",
                legs=[leg], basis_base_units=actual_basis,
                expected_payout=qty * 1.0,
                edge_pct=round(edge * 100, 3),
                event_id=event.id,
                city=scan.city,
                station_id=station["station_id"],
                p_model=round(p, 4),
                expected_unlock_ts=expected_unlock,
                prefer_provider=bool(intraday_cfg.get("prefer_provider", True)),
                provider_wait_sec=provider_wait_sec,
                raw_snapshot={
                    "kind": "intraday_metar_lock",
                    "observed_high_c": obs,
                    "bucket_lo_c": b.lo_c,
                    "bucket_hi_c": b.hi_c,
                    "bucket_label": b.label,
                    "post_peak_sigma_c": post_peak_sigma_c,
                    "upper_bound": upper_bound,
                    "p_eff": p,
                    "edge_net": edge,
                    "kelly_tier": kelly_tier,
                    "n_obs": summary.n_observations,
                    "last_obs_utc": (
                        summary.last_obs_utc.isoformat() if summary.last_obs_utc else None
                    ),
                },
            )
            await emit(opp)
            n_emitted += 1
            logger.info(
                "E4 INTRADAY_LOCK %s obs=%.1f°C bucket=[%.1f, %.1f] upper_bound=%.3f "
                "p=%.3f edge=%.2f%% qty=%d",
                scan.city, obs, b.lo_c, b.hi_c, upper_bound, p, edge * 100, qty,
            )

    return n_emitted


async def intraday_metar_loop(
    *,
    emit: Callable[[Opportunity], Awaitable[None]],
    available_resource_provider: Callable[[], float],
) -> None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(4)
    intraday_cfg = cfg.get("intraday_metar", {}) or {}
    interval = float(intraday_cfg.get("interval_sec", 60.0))
    backoff = Backoff(base=interval, cap=600.0)
    while True:
        try:
            async with (
                GammaClient() as gamma,
                NetworkClient() as network,
                MetarClient() as metar,
            ):
                n = await intraday_metar_scan_once(
                    gamma=gamma, network=network, metar=metar,
                    emit=emit, available_resource_provider=available_resource_provider,
                )
            if n > 0:
                logger.info("E4 intraday_metar pass — %d opportunities emitted", n)
            net_circuit.record_success()
            backoff.reset()
            await asyncio.sleep(interval)
        except Exception as exc:
            delay = backoff.next()
            if is_network_error(exc):
                net_circuit.record_failure()
                logger.warning("E4 intraday_metar net unavailable (%s) — sleep %.0fs",
                               type(exc).__name__, delay)
            else:
                logger.exception("E4 intraday_metar iteration failed")
            await asyncio.sleep(delay)
