"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

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
class _ConvergeState:
    seen_keys: deque = field(default_factory=lambda: deque(maxlen=2000))


_state = _ConvergeState()


async def convergence_event_resolver_scan_once(
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
    event_resolver_cfg = cfg.get("convergence_event_resolver", {}) or {}
    if not event_resolver_cfg.get("enabled", True):
        return 0

    min_hours = float(event_resolver_cfg.get("min_hours_to_resolution", 0.5))
    max_hours = float(event_resolver_cfg.get("max_hours_to_resolution", 4.0))
    edge_min = float(event_resolver_cfg.get("min_edge", 0.08))
    max_per_execution = float(event_resolver_cfg.get("max_per_execution_base_units", 1.0))
    base_sigma_c = float(event_resolver_cfg.get("converge_sigma_c", 0.6))
    max_sigma_c = float(event_resolver_cfg.get("converge_sigma_max_c", 1.5))
    min_upper_bound = float(event_resolver_cfg.get("min_upper_bound", 0.05))
    max_upper_bound = float(event_resolver_cfg.get("max_upper_bound", 0.60))
    provider_wait_sec = float(event_resolver_cfg.get("provider_wait_sec", 20.0))
    min_payload_base_units = float(CONFIG.globals.get("min_payload_base_units_live", 1.05))
    from ...config import ENV

    try:
        events = await gamma.list_events(active=True, closed=False, limit=300)
    except Exception as exc:
        if is_network_error(exc):
            net_circuit.record_failure()
            raise
        return 0

    weather_events = [e for e in events if is_weather_event(e)]
    if not weather_events:
        return 0

    n_emitted = 0
    resource = available_resource_provider()
    now_utc = datetime.now(timezone.utc)
    seen_cities: set[tuple[str, str]] = set()

    for event in weather_events:
        scan = scan_event(event)
        if not scan or scan.kind != KIND_TEMPERATURE:
            continue
        city_key = (scan.city, event.id)
        if city_key in seen_cities:
            continue
        seen_cities.add(city_key)

        # Hours-to-resolution gate (the distinguishing filter)
        try:
            end_dt = datetime.fromisoformat(scan.target_date_iso.replace("Z", "+00:00"))
            hours_to = (end_dt - now_utc).total_seconds() / 3600.0
        except Exception:
            continue
        if hours_to < min_hours or hours_to > max_hours:
            continue

        station = scan.station
        target = scan.target_date_iso[:10] if scan.target_date_iso else None
        if not target:
            continue

        # F11 fix: scale sigma with remaining time to resolution.
        # At T=min_hours: use base_sigma (tight — observation is nearly final).
        # At T=max_hours: use max_sigma (wider — peak may not have occurred yet).
        # Linear interpolation prevents the event_resolver from using 0.6°C at T=4h
        # when the city hasn't reached its daily peak.
        time_frac = (hours_to - min_hours) / max(max_hours - min_hours, 0.01)
        sigma_c = base_sigma_c + (max_sigma_c - base_sigma_c) * min(1.0, max(0.0, time_frac))

        # F11 fix: additionally require that we are past the peak window OR
        # hours_to < 1h (event_node resolving soon regardless of peak).
        # Pre-peak at T > 1h: observation is still rising — event_resolver fires
        # on the current value which is NOT the day's final high.
        try:
            from zoneinfo import ZoneInfo
            from datetime import time as _time
            pk_end = station.get("peak_window_local", ["14:00", "16:00"])[1]
            ph, pm = (int(x) for x in pk_end.split(":"))
            local_now = now_utc.astimezone(ZoneInfo(station["timezone"])).time()
            past_peak = local_now >= _time(ph, pm)
        except Exception:
            past_peak = True  # fail open — don't block on tz error
        if not past_peak and hours_to > 1.0:
            logger.debug("E4 convergence_event_resolver skip %s: pre-peak, hours_to=%.1f", scan.city, hours_to)
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

        typical_lag = float(station.get("typical_resolution_lag_hours", 4.0))
        expected_unlock = end_dt + timedelta(hours=typical_lag)

        for b in scan.buckets:
            upper_bound = book_map.get(b.yes_unit_id)
            if upper_bound is None or upper_bound < min_upper_bound or upper_bound > max_upper_bound:
                continue
            if obs >= b.hi_c:
                continue
            p = p_bucket(obs, sigma_c, b.lo_c, b.hi_c)
            # Convergence threshold — we want strong conviction this late
            min_p_for_emit = float(event_resolver_cfg.get("min_p_conviction", 0.70))
            if p < min_p_for_emit:
                continue
            edge = net_edge_pct(p, upper_bound) / 100.0
            if edge < edge_min:
                continue
            # Dedup includes the hour-bucket so we re-emit if observation
            # changes meaningfully (e.g. late afternoon swing).
            hour_bucket = int(hours_to * 2)  # 30-min granularity
            key = f"converge:{scan.city}:{event.id}:{b.event_node.id}:{round(obs, 1)}:{hour_bucket}"
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
                engine="E4", kind="WEATHER_BOOK_CONVERGE",
                legs=[leg], basis_base_units=actual_basis,
                expected_payout=qty * 1.0,
                edge_pct=round(edge * 100, 3),
                event_id=event.id,
                city=scan.city,
                station_id=station["station_id"],
                p_model=round(p, 4),
                expected_unlock_ts=expected_unlock,
                prefer_provider=bool(event_resolver_cfg.get("prefer_provider", True)),
                provider_wait_sec=provider_wait_sec,
                raw_snapshot={
                    "kind": "book_convergence",
                    "observed_high_c": obs,
                    "bucket_lo_c": b.lo_c,
                    "bucket_hi_c": b.hi_c,
                    "bucket_label": b.label,
                    "hours_to_resolution": round(hours_to, 2),
                    "upper_bound": upper_bound,
                    "p_eff": p,
                    "converge_sigma_c": round(sigma_c, 3),
                    "past_peak": past_peak,
                    "edge_net": edge,
                    "kelly_tier": kelly_tier,
                    "n_obs": summary.n_observations,
                },
            )
            await emit(opp)
            n_emitted += 1
            logger.info(
                "E4 BOOK_CONVERGE %s t-%.1fh obs=%.1f°C [%.1f, %.1f] "
                "upper_bound=%.3f p=%.3f edge=%.2f%%",
                scan.city, hours_to, obs, b.lo_c, b.hi_c, upper_bound, p, edge * 100,
            )
    return n_emitted


async def convergence_event_resolver_loop(
    *,
    emit: Callable[[Opportunity], Awaitable[None]],
    available_resource_provider: Callable[[], float],
) -> None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(4)
    event_resolver_cfg = cfg.get("convergence_event_resolver", {}) or {}
    interval = float(event_resolver_cfg.get("interval_sec", 30.0))
    backoff = Backoff(base=interval, cap=600.0)
    while True:
        try:
            async with (
                GammaClient() as gamma,
                NetworkClient() as network,
                MetarClient() as metar,
            ):
                n = await convergence_event_resolver_scan_once(
                    gamma=gamma, network=network, metar=metar,
                    emit=emit, available_resource_provider=available_resource_provider,
                )
            if n > 0:
                logger.info("E4 convergence_event_resolver pass — %d opportunities emitted", n)
            net_circuit.record_success()
            backoff.reset()
            await asyncio.sleep(interval)
        except Exception as exc:
            delay = backoff.next()
            if is_network_error(exc):
                net_circuit.record_failure()
                logger.warning("E4 convergence_event_resolver net unavailable (%s) — sleep %.0fs",
                               type(exc).__name__, delay)
            else:
                logger.exception("E4 convergence_event_resolver iteration failed")
            await asyncio.sleep(delay)
