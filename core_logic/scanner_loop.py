"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable

from ...common import net_circuit
from ...common.net_errors import Backoff, is_network_error
from ...config import CONFIG
from ..opportunity import Opportunity
from ...ingestion.network_client import NetworkClient
from ...ingestion.gamma_client import GammaClient
from ...ingestion.weather.ensemble_client import EnsembleClient
from ...ingestion.weather.historical_client import HistoricalClient
from ...ingestion.weather.metar_client import MetarClient
from ...ingestion.weather.open_meteo_client import OpenMeteoClient
from ..opportunity import Opportunity
from .edge_detector import detect_for_event
from .flash_scorer import FlashScorer
from .event_node_scanner import find_precip_events, find_weather_events, scan_event, scan_precip_event
from .struct_scanner import detect_struct

logger = logging.getLogger(__name__)

# F17: per-(event_id, unit_id) emit timestamp for dedup.
# Prevents re-emitting the same bucket on every 5-min scan pass.
_recent_emits: dict[tuple[str, str], float] = {}


async def _emit_dedup(
    opp: "Opportunity",
    emit: Callable[["Opportunity"], Awaitable[None]],
    cooldown_sec: float,
) -> bool:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    global _recent_emits
    now = time.monotonic()
    unit_id = opp.legs[0].unit_id if opp.legs else ""
    key = (opp.event_id or "", unit_id)
    if now - _recent_emits.get(key, 0.0) < cooldown_sec:
        return False
    _recent_emits[key] = now
    # Prune stale entries to avoid unbounded growth
    if len(_recent_emits) > 2000:
        cutoff = now - cooldown_sec * 2
        _recent_emits = {k: v for k, v in _recent_emits.items() if v > cutoff}
    await emit(opp)
    return True


async def scan_weather_event_nodes_once(
    *,
    available_resource_base_units: float,
    flash_scorer: FlashScorer,
    emit: Callable[[Opportunity], Awaitable[None]],
) -> int:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if net_circuit.is_open():
        logger.debug("E4 skipped — net circuit open (%.0fs left)", net_circuit.time_remaining())
        return 0
    n_emitted = 0
    cfg = CONFIG.engine(4)
    dedup_cooldown = float(cfg.get("event_dedup_cooldown_sec", 300.0))

    async with GammaClient() as gamma, OpenMeteoClient() as om, EnsembleClient() as en, HistoricalClient() as hc, MetarClient() as me, NetworkClient() as network:
        temp_events = await find_weather_events(gamma)
        precip_events = await find_precip_events(gamma)
        logger.info("E4 found %d temp events, %d precip events", len(temp_events), len(precip_events))

        # F6 fix: process temperature and precipitation events with their own
        # parsers. The old `scan_event(e) or scan_precip_event(e)` routing
        # caused precipitation events (which match temperature keywords) to be
        # parsed as temperature events — mm values treated as °C in the model.
        seen_event_ids: set[str] = set()

        async def _process_scan(event, scan):
            nonlocal n_emitted
            if not scan:
                return
            # Refresh bucket upper_bounds from live network payload book
            unit_ids = [b.yes_unit_id for b in scan.buckets if b.yes_unit_id]
            if unit_ids:
                try:
                    books = await network.get_books(unit_ids)
                    book_map = {bk.unit_id: bk.best_upper_bound for bk in books}
                    for bucket in scan.buckets:
                        live_upper_bound = book_map.get(bucket.yes_unit_id)
                        if live_upper_bound is not None:
                            bucket.yes_upper_bound = live_upper_bound
                except Exception:
                    logger.debug("E4 network upper_bound refresh failed for %s — using gamma metrics", event.title)
            # E4-STRUCT first (riskless)
            struct = detect_struct(scan)
            if struct:
                if await _emit_dedup(struct, emit, dedup_cooldown):
                    n_emitted += 1
            # E4-MODEL (Phase 2A — may emit multiple +EV buckets per event)
            try:
                opps = await detect_for_event(
                    scan,
                    open_meteo=om, ensemble=en, historical=hc, metar=me,
                    flash_scorer=flash_scorer,
                    available_resource_base_units=available_resource_base_units,
                )
            except Exception as exc:
                if is_network_error(exc):
                    raise
                logger.exception("E4 detect_for_event failed for %s", event.title)
                return
            for opp in opps:
                if await _emit_dedup(opp, emit, dedup_cooldown):
                    n_emitted += 1

        for event in temp_events:
            if event.id in seen_event_ids:
                continue
            seen_event_ids.add(event.id)
            scan = scan_event(event)
            await _process_scan(event, scan)

        for event in precip_events:
            if event.id in seen_event_ids:
                continue
            seen_event_ids.add(event.id)
            scan = scan_precip_event(event)
            await _process_scan(event, scan)

    return n_emitted


async def scan_weather_loop(
    *,
    available_resource_provider: Callable[[], float],
    emit: Callable[[Opportunity], Awaitable[None]],
) -> None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    cfg = CONFIG.engine(4)
    interval = max(60, int(cfg["scan_interval_minutes"]) * 60)
    flash = FlashScorer()
    backoff = Backoff(base=float(interval), cap=600.0)
    while True:
        try:
            n = await scan_weather_event_nodes_once(
                available_resource_base_units=available_resource_provider(),
                flash_scorer=flash,
                emit=emit,
            )
            logger.info("E4 scan done — %d opportunities emitted", n)
            net_circuit.record_success()
            backoff.reset()
            await asyncio.sleep(interval)
        except Exception as exc:
            delay = backoff.next()
            if is_network_error(exc):
                net_circuit.record_failure()
                logger.warning("E4 network unavailable (%s) — sleep %.0fs", type(exc).__name__, delay)
            else:
                logger.exception("E4 scan loop iteration failed")
            await asyncio.sleep(delay)
