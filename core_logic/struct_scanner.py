"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

from typing import Optional

from ..opportunity import Leg, Opportunity
from ...common.gas_costs import net_edge_pct
from ...config import CONFIG
from .event_node_scanner import WeatherEventScan


def detect_struct(scan: WeatherEventScan, *, threshold: float = 0.965) -> Optional[Opportunity]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    upper_bounds = [b.yes_upper_bound for b in scan.buckets]
    if any(a is None for a in upper_bounds):
        return None
    s = sum(upper_bounds)
    if s >= threshold:
        return None

    # Edge-aware sizing: scale qty so total basis stays within max_per_execution_base_units.
    # Default qty is 1 (minimum for structural arb), but we scale up when
    # edge is large and resource allows.
    cfg = CONFIG.engine(4)
    max_per_execution = float(cfg.get("max_per_execution_base_units", 2.50))
    qty = max(1, int(max_per_execution / s)) if s > 0 else 1

    edge_pct = net_edge_pct(payout_per_unit=1.0, basis_per_unit=s)
    legs = [
        Leg(
            unit_id=b.yes_unit_id,
            side="YES",
            metric=float(b.yes_upper_bound),
            qty=qty,
            event_node_id=b.event_node.id,
            event_node_title=b.event_node.question,
        )
        for b in scan.buckets
    ]
    return Opportunity(
        engine="E4",
        kind="WEATHER_STRUCT_UNDER",
        legs=legs,
        basis_base_units=round(s * qty, 4),
        expected_payout=float(qty),
        edge_pct=round(edge_pct, 3),
        event_id=scan.event.id,
        city=scan.city,
        station_id=scan.station["station_id"],
        raw_snapshot={"sum_yes": s, "n_buckets": len(scan.buckets), "qty": qty},
    )