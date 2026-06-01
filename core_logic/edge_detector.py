"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from ... import db
from ...config import CONFIG
from ...ingestion.weather.ensemble_client import EnsembleClient
from ...ingestion.weather.historical_client import HistoricalClient
from ...ingestion.weather.metar_client import MetarClient
from ...ingestion.weather.open_meteo_client import OpenMeteoClient
from ...ingestion.weather.station_registry import STATIONS
from ..opportunity import Leg, Opportunity
from ...common.gas_costs import net_edge_pct
from .consensus_builder import build_consensus
from .flash_scorer import FlashScorer
from .event_node_scanner import KIND_PRECIPITATION, WeatherEventScan
from .probability_estimator import (apply_live_observation, blend_gaussian_empirical,
                                    boundary_penalty, divergence_size_multiplier,
                                    empirical_p_bucket_from_members,
                                    enforce_live_min_qty,
                                    fat_tail_var_multiplier, kelly_fraction,
                                    kelly_tier_fraction,
                                    p_bucket, poisson_tail_boost,
                                    shift_members_to_observation)

logger = logging.getLogger(__name__)


def _data_age_size_multiplier(age_min: float) -> float:
    cfg = CONFIG.engine(4)
    if age_min <= cfg["max_data_age_min_full_size"]:
        return 1.0
    if age_min <= cfg["max_data_age_min_half_size"]:
        return 0.5
    if age_min <= cfg["max_data_age_min_quarter_size"]:
        return 0.25
    return 0.0


def _reject(scan, reason: str, **kwargs) -> None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    extras = " ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info("E4 rejected %s [%s]: %s %s", scan.city or "?", scan.station["station_id"], reason, extras)


def _is_extreme_event(consensus: float, clim_avg: float, clim_std: float, threshold: float) -> bool:
    if clim_std <= 0:
        return False
    return abs(consensus - clim_avg) / clim_std > threshold


async def detect_for_precip_event(
    scan: WeatherEventScan,
    *,
    ensemble: EnsembleClient,
    historical: HistoricalClient,
    available_resource_base_units: float,
    flash_scorer: Optional[FlashScorer] = None,
) -> list[Opportunity]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    from datetime import timedelta
    cfg = CONFIG.engine(4)
    station = scan.station

    try:
        end_dt = datetime.fromisoformat(scan.target_date_iso.replace("Z", "+00:00"))
        hours_to = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0
    except Exception:
        _reject(scan, "precip end_date parse failed")
        return []
    if hours_to < cfg["min_hours_to_resolution"] or hours_to > cfg["max_hours_to_resolution"]:
        _reject(scan, "precip hours_to out of band", hours_to=round(hours_to, 1))
        return []

    today = datetime.now(timezone.utc).date()
    month_start = today.replace(day=1).isoformat()
    yesterday = (today - timedelta(days=1)).isoformat()
    month_end = scan.target_date_iso[:10]

    elapsed_mm, forecast = await asyncio.gather(
        historical.monthly_precip_elapsed(
            station["lat"], station["lon"],
            month_start=month_start, yesterday=yesterday,
        ),
        ensemble.monthly_precip_forecast(
            station["lat"], station["lon"],
            start_date=today.isoformat(), end_date=month_end,
        ),
    )

    if forecast.n_members < 5:
        _reject(scan, "too few precip ensemble members", n_members=forecast.n_members)
        return []

    total_est_mm = elapsed_mm + forecast.mean_mm
    sigma_mm = max(forecast.sigma_mm, 1.0)

    edge_min = cfg["min_edge_threshold"]
    max_per_execution = cfg.get("max_per_execution_base_units_precip", cfg["max_per_execution_base_units"])
    pool_pct = cfg.get("max_pool_allocation_pct_precip", cfg["max_pool_allocation_pct"])

    candidates: list[dict] = []
    for b in scan.buckets:
        if b.yes_upper_bound is None or b.yes_upper_bound <= 0.01 or b.yes_upper_bound >= 0.95:
            continue
        p = p_bucket(total_est_mm, sigma_mm, b.lo_c, b.hi_c)
        edge = net_edge_pct(p, b.yes_upper_bound) / 100.0
        if edge < edge_min:
            continue
        candidates.append({"bucket": b, "edge": edge, "p": p})

    if not candidates:
        _reject(scan, "no precip bucket beat edge_min",
                edge_min=round(edge_min, 3),
                total_mm=round(total_est_mm, 1), sigma_mm=round(sigma_mm, 1))
        return []

    candidates.sort(key=lambda x: x["edge"], reverse=True)
    max_emits = int(cfg.get("max_emits_per_event", 4))
    candidates = candidates[:max_emits]

    # F10 fix: apply flash LLM veto to precipitation events (was bypassed before)
    risk_mult = 1.0
    if flash_scorer is not None and candidates:
        top = candidates[0]
        b_top = top["bucket"]
        try:
            rs = await flash_scorer.score({
                "city": scan.city,
                "target_date": scan.target_date_iso[:10] if scan.target_date_iso else "",
                "consensus_c": round(total_est_mm, 1),
                "sigma_c": round(sigma_mm, 1),
                "observed_high_c": "N/A",
                "hours_to_resolution": round(hours_to, 1),
                "bucket_lo_c": b_top.lo_c,
                "bucket_hi_c": b_top.hi_c,
                "p_scipy": round(top["p"], 4),
                "upper_bound": b_top.yes_upper_bound,
                "implied_pct": f"{b_top.yes_upper_bound * 100:.1f}%",
                "hemisphere": "N/A",
                "season": "N/A",
            })
            risk_mult = rs.size_multiplier
            if risk_mult <= 0.0:
                _reject(scan, "precip flash risk veto")
                return []
        except Exception:
            logger.debug("E4 precip flash scorer failed — using default risk_mult=1.0")

    expected_unlock = end_dt + timedelta(hours=float(station.get("typical_resolution_lag_hours", 48.0)))
    opportunities: list[Opportunity] = []

    # F9 fix: cap total basis across all precip buckets to global max_exposure_per_event
    max_event_exposure = float(CONFIG.globals.get("max_exposure_per_event_base_units", 3.0))
    event_basis_so_far = 0.0

    from ...config import ENV
    min_payload_base_units = float(CONFIG.globals.get("min_payload_base_units_live", 1.05))

    for c in candidates:
        b = c["bucket"]
        edge = c["edge"]
        p_b = c["p"]
        kelly_tier = kelly_tier_fraction(edge)
        kf = kelly_fraction(p_b, b.yes_upper_bound)
        raw = kelly_tier * kf * available_resource_base_units
        remaining_event_budget = max_event_exposure - event_basis_so_far
        basis = min(raw, max_per_execution, pool_pct * available_resource_base_units, remaining_event_budget)
        basis *= risk_mult
        if basis < CONFIG.globals["min_execution_base_units"]:
            continue
        qty = int(basis / b.yes_upper_bound)
        qty = enforce_live_min_qty(qty, b.yes_upper_bound, bool(ENV.paper_execution), min_payload_base_units)
        if qty < 1:
            continue
        actual_basis = round(qty * b.yes_upper_bound, 4)
        event_basis_so_far += actual_basis
        leg = Leg(
            unit_id=b.yes_unit_id, side="YES",
            metric=float(b.yes_upper_bound), qty=qty,
            event_node_id=b.event_node.id, event_node_title=b.event_node.question,
        )
        opportunities.append(Opportunity(
            engine="E4", kind="WEATHER_PRECIP_MODEL",
            legs=[leg], basis_base_units=actual_basis,
            expected_payout=qty * 1.0,
            edge_pct=round(edge * 100, 3),
            event_id=scan.event.id, city=scan.city,
            station_id=station["station_id"],
            p_model=round(p_b, 4),
            expected_unlock_ts=expected_unlock,
            raw_snapshot={
                "kind": "precipitation",
                "bucket_label": b.label,
                "bucket_lo_mm": b.lo_c, "bucket_hi_mm": b.hi_c,
                "upper_bound": b.yes_upper_bound,
                "elapsed_mm": elapsed_mm,
                "forecast_mean_mm": forecast.mean_mm,
                "total_est_mm": round(total_est_mm, 1),
                "sigma_mm": round(sigma_mm, 1),
                "n_members": forecast.n_members,
                "days_remaining": forecast.days_remaining,
                "edge_net": edge,
                "kelly_tier": kelly_tier,
                "multi_bucket_rank": candidates.index(c) + 1,
                "multi_bucket_total": len(candidates),
            },
        ))

    return opportunities


async def detect_for_event(
    scan: WeatherEventScan,
    *,
    open_meteo: OpenMeteoClient,
    ensemble: EnsembleClient,
    historical: HistoricalClient,
    metar: MetarClient,
    flash_scorer: Optional[FlashScorer],
    available_resource_base_units: float,
) -> list[Opportunity]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if scan.kind == KIND_PRECIPITATION:
        return await detect_for_precip_event(
            scan, ensemble=ensemble, historical=historical,
            available_resource_base_units=available_resource_base_units,
            flash_scorer=flash_scorer,
        )
    cfg = CONFIG.engine(4)
    station = scan.station
    target = scan.target_date_iso[:10] if scan.target_date_iso else ""
    if not target:
        _reject(scan, "no target date")
        return []

    # Time-to-resolution gate
    try:
        end_dt = datetime.fromisoformat(scan.target_date_iso.replace("Z", "+00:00"))
        hours_to = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0
    except Exception:
        _reject(scan, "end_date parse failed", target=scan.target_date_iso)
        return []
    if hours_to < cfg["min_hours_to_resolution"] or hours_to > cfg["max_hours_to_resolution"]:
        _reject(
            scan, "hours_to out of band",
            hours_to=round(hours_to, 1),
            min=cfg["min_hours_to_resolution"], max=cfg["max_hours_to_resolution"],
        )
        return []

    # Manual-resolution weekend lockout (Loophole #6)
    if station.get("manual_resolution"):
        weekday = end_dt.weekday()       # 0=Mon, 6=Sun
        if weekday in (4, 5, 6) and hours_to < cfg["weekend_resolution_lockout_hours"]:
            _reject(scan, "weekend resolution lockout", weekday=weekday, hours_to=round(hours_to, 1))
            return []

    # Parallel data fetch
    from datetime import date as _date
    try:
        td = _date.fromisoformat(target)
    except ValueError:
        _reject(scan, "target date parse failed", target=target)
        return []
    metar_summary_tupper_bound = metar.daily_summary(station["station_id"], target, station["timezone"])
    open_meteo_tupper_bound = open_meteo.fetch_daily_highs(station["lat"], station["lon"])
    # 2026-05-17: switched from daily_divergence() to daily_distribution() to obtain
    # the full per-member empirical CDF — required for Singapore-style tail-bin
    # detection where Gaussian under-estimates bimodal/skewed mass.
    ensemble_tupper_bound = ensemble.daily_distribution(station["lat"], station["lon"], target)
    clim_tupper_bound = historical.calendar_day_climatology(station["lat"], station["lon"], td.month, td.day)
    metar_summary, mh, ens_dist, clim = await asyncio.gather(
        metar_summary_tupper_bound, open_meteo_tupper_bound, ensemble_tupper_bound, clim_tupper_bound,
        return_exceptions=False,
    )

    age_size = _data_age_size_multiplier(mh.data_age_min)
    if age_size == 0.0:
        _reject(scan, "forecast data too stale", data_age_min=round(mh.data_age_min, 1))
        return []

    # Consensus (db pulls applied bias / kappa)
    bias_offset_c, _n = db.get_station_bias(station["station_id"])
    if bias_offset_c == 0.0:
        bias_offset_c = float(station.get("bias_offset_c", 0.0))
    kappa = db.get_station_kappa(station["station_id"], default=cfg["default_dispersion_kappa"])

    by_model = mh.for_date(target)
    # Use empirical σ from the ensemble distribution (std across all members).
    # Falls back to 2.0 if too few members for stable std (matches old behaviour).
    ensemble_divergence_c = ens_dist.std_c if ens_dist.n_members >= 5 else 2.0
    consensus = build_consensus(
        by_model_high_c=by_model,
        weights=station["model_weights"],
        ensemble_divergence=ensemble_divergence_c,
        clim_avg=clim.avg_c,
        clim_std=clim.std_c,
        bias_offset_c=bias_offset_c,
        dispersion_kappa=kappa,
    )

    if consensus.confidence == "low" or consensus.models_used == 0:
        _reject(
            scan, "low confidence or zero models",
            confidence=consensus.confidence, models_used=consensus.models_used,
        )
        return []
    if consensus.model_agreement_score < cfg["min_model_agreement_score"]:
        _reject(
            scan, "model agreement below threshold",
            agreement=round(consensus.model_agreement_score, 3),
            min=cfg["min_model_agreement_score"],
        )
        return []

    # Phase 4A (2026-05-19): Bayesian σ tightening using realized error.
    # When the station has ≥10 days of accuracy data, blend the kappa-inflated
    # σ with the realized σ via geometric mean. Stations whose models have
    # been MORE accurate than expected get a tighter σ → biases Kelly UP.
    # Stations with WIDER realized error get a looser σ → biases Kelly DOWN.
    import math as _math
    from ...config import ENV
    realized = db.get_station_realized_sigma(station["station_id"], days=14, min_samples=10)
    bayesian_sigma_used = False
    sigma_for_adjust = consensus.effective_sigma
    if realized is not None:
        realized_sigma, n_days = realized
        # F8 fix: Bayesian combination of independent precision sources.
        # 1/σ_blended² = 1/σ_eff² + 1/σ_realized²  (harmonic mean of precisions).
        # The old geometric mean violated σ_blended ≤ min(σ_eff, σ_realized) when
        # realized errors were small — it inflated sigma, killing Kelly on accurate
        # stations. Correct Bayesian combination always reduces uncertainty.
        prec_eff = 1.0 / max(consensus.effective_sigma ** 2, 0.01)
        prec_real = 1.0 / max(realized_sigma ** 2, 0.01)
        blended = max(0.5, 1.0 / _math.sqrt(prec_eff + prec_real))
        sigma_for_adjust = blended
        bayesian_sigma_used = True

    # Live observation override (Loophole #2)
    adj = apply_live_observation(
        consensus.consensus_temp,
        sigma_for_adjust,
        metar_summary.observed_high_c,
        datetime.now(timezone.utc),
        station["timezone"],
        tuple(station["peak_window_local"]),
    )

    # B2 fix (2026-05-19): rebase ensemble members to the realized starting
    # point so empirical CDF matches the live-shifted Gaussian. Without this
    # the blend silently kills high-conviction "already exceeded" executions.
    members_for_empirical = shift_members_to_observation(
        ens_dist.members_c,
        original_consensus_c=consensus.consensus_temp,
        adjusted_consensus_c=adj.consensus_c,
        observed_high_c=metar_summary.observed_high_c,
    )

    # Extreme-event throttle
    extreme = _is_extreme_event(
        adj.consensus_c, clim.avg_c, clim.std_c, cfg["extreme_event_z_threshold"],
    )
    max_per_execution = cfg["max_per_execution_base_units"]
    if extreme:
        max_per_execution = min(max_per_execution, cfg["extreme_event_max_per_execution_base_units"])

    # Edge threshold (manual-resolution event_nodes need higher edge)
    edge_min = (
        cfg["min_edge_threshold_manual_resolution"] if station.get("manual_resolution")
        else cfg["min_edge_threshold"]
    )

    # F15: pre-compute hemisphere/season for flash scorer context
    import calendar as _cal
    _lat = station.get("lat", 0.0)
    _hemisphere = "Northern" if _lat >= 0 else "Southern"
    _month = end_dt.month
    _season_n = {(3,4,5):"Spring",(6,7,8):"Summer",(9,10,11):"Autumn",(12,1,2):"Winter"}
    _season_s = {(3,4,5):"Autumn",(6,7,8):"Winter",(9,10,11):"Spring",(12,1,2):"Summer"}
    _seas_map = _season_n if _hemisphere == "Northern" else _season_s
    _season = next((s for ks, s in _seas_map.items() if _month in ks), "Unknown")

    # 2026-05-17: Three-stage probability estimation per bucket (Singapore lesson).
    #   1. p_gaussian   — original Normal(consensus, σ) CDF (existing logic)
    #   2. p_empirical  — direct count over ensemble members (catches bimodal mass)
    #   3. p_blended    — weighted fuse, weight rises with HIGH_DIV regime
    #   4. p_effective  — Poisson tail boost on rare bins the event_node under-metrics
    #
    # `p_effective` then drives both edge calculation AND Kelly sizing.
    # Sizing is FURTHER adjusted by divergence_size_multiplier (size up on HIGH_DIV
    # tails, down on HIGH_DIV mid bins) and fat_tail_var_multiplier (cuts size on
    # catastrophic / extreme combinations). None of these are gates — frequency
    # is preserved; the additions only shape *how much* to allocate, not whether.
    tail_pricing_cfg = cfg.get("tail_pricing", {}) or {}
    blend_enabled = tail_pricing_cfg.get("empirical_blend_enabled", True)
    poisson_enabled = tail_pricing_cfg.get("poisson_tail_boost_enabled", True)
    max_upper_bound_boost = float(tail_pricing_cfg.get("poisson_max_upper_bound_for_boost", 0.15))
    max_boost_factor = float(tail_pricing_cfg.get("poisson_max_boost_factor", 2.0))

    # Phase 2A (2026-05-19): collect ALL positive-EV buckets, not just best.
    # Per-bucket (b, edge_net, p_eff, p_gauss, p_emp) tuples for later sizing.
    candidates: list[dict] = []
    for b in scan.buckets:
        if b.yes_upper_bound is None or b.yes_upper_bound <= 0.01 or b.yes_upper_bound >= 0.95:
            continue
        # Hard P=0 if observed already exceeds bucket (upper bound) or
        # is far enough below bucket's lower bound (2σ below lo_c)
        if metar_summary.observed_high_c is not None and metar_summary.observed_high_c >= b.hi_c:
            continue
        if (
            metar_summary.observed_high_c is not None
            and adj.sigma_c > 0
            and metar_summary.observed_high_c < b.lo_c
            and (b.lo_c - metar_summary.observed_high_c) > 2 * adj.sigma_c
        ):
            continue
        # Stage 1 — Gaussian (F2 fix: boundary_penalty stored for sizing, not applied to p)
        p_gauss = p_bucket(adj.consensus_c, adj.sigma_c, b.lo_c, b.hi_c)
        bp = boundary_penalty(adj.consensus_c, b.lo_c, b.hi_c, adj.sigma_c)
        # Stage 2 — empirical (uses live-shifted members; B2 fix)
        if blend_enabled and ens_dist.n_members >= 5:
            p_emp = empirical_p_bucket_from_members(members_for_empirical, b.lo_c, b.hi_c)
            p_blended = blend_gaussian_empirical(
                p_gauss, p_emp, ens_dist.n_members, consensus.dispersion_regime,
            )
        else:
            p_emp = 0.0
            p_blended = p_gauss
        # Stage 3 — ensemble-evidence boost (poisson_tail_boost is now a no-op after F1 fix)
        if poisson_enabled:
            p_eff = poisson_tail_boost(
                p_blended, b.yes_upper_bound, consensus.dispersion_regime, ens_dist.n_members,
                max_upper_bound_for_boost=max_upper_bound_boost, max_boost_factor=max_boost_factor,
            )
        else:
            p_eff = p_blended
        edge_net = net_edge_pct(p_eff, b.yes_upper_bound) / 100.0
        if edge_net < edge_min:
            continue
        candidates.append({
            "bucket": b, "edge_net": edge_net,
            "p_eff": p_eff, "p_gauss": p_gauss, "p_emp": p_emp,
            "boundary_mult": bp,
        })

    if not candidates:
        _reject(
            scan, "no bucket beat edge_min",
            edge_min=round(edge_min, 3), n_buckets=len(scan.buckets),
            consensus_c=round(adj.consensus_c, 2),
            regime=consensus.dispersion_regime,
        )
        return []

    # Sort by edge desc; cap total emissions per event (config: max_emits_per_event, default 4).
    candidates.sort(key=lambda x: x["edge_net"], reverse=True)
    max_emits = int(cfg.get("max_emits_per_event", 4))
    candidates = candidates[:max_emits]

    # F3 fix: correlation discount for multi-bucket emissions.
    # Buckets in the same event are mutually exclusive outcomes — probabilistic_allocation 4 at
    # full Kelly is equivalent to 4× correlated concentration. Scale Kelly down
    # for each successive bucket so total event exposure stays sane.
    corr_discounts = cfg.get("multi_bucket_correlation_discounts", [1.0, 0.5, 0.33, 0.25])
    for i, c in enumerate(candidates):
        c["corr_discount"] = float(corr_discounts[min(i, len(corr_discounts) - 1)])

    # Flash risk overlay — called ONCE with the top-edge bucket (advisory only
    # for the event). Result applied uniformly across all emitted buckets to
    # avoid N expensive Gemini calls per event.
    top = candidates[0]
    risk_mult = 1.0
    risk_source = "default"
    risk_reason = ""
    if flash_scorer is not None:
        rs = await flash_scorer.score({
            "city": scan.city,
            "target_date": target,
            "consensus_c": adj.consensus_c,
            "sigma_c": adj.sigma_c,
            "observed_high_c": metar_summary.observed_high_c,
            "hours_to_resolution": round(hours_to, 1),
            "bucket_lo_c": top["bucket"].lo_c,
            "bucket_hi_c": top["bucket"].hi_c,
            "p_scipy": round(top["p_eff"], 4),
            "upper_bound": top["bucket"].yes_upper_bound,
            "implied_pct": f"{top['bucket'].yes_upper_bound * 100:.1f}%",
            "hemisphere": _hemisphere,
            "season": _season,
        })
        risk_mult = rs.size_multiplier
        risk_source = rs.source
        risk_reason = rs.risk_reason
        if risk_mult <= 0.0:
            _reject(scan, "flash risk veto", risk_source=risk_source, risk_reason=risk_reason)
            return []

    typical_lag = float(station.get("typical_resolution_lag_hours", 4.0))
    from datetime import timedelta
    expected_unlock = end_dt + timedelta(hours=typical_lag)

    # Per-bucket sizing + emission. Tiered Kelly (Phase 1D) scales the fraction
    # by edge magnitude — thin edges get quarter-Kelly, fat tails get 0.8×.
    # Per-event cap is handled by resource_allocator's MAX_EXPOSURE_PER_EVENT_BASE_UNITS.
    opportunities: list[Opportunity] = []
    min_payload_base_units = float(CONFIG.globals.get("min_payload_base_units_live", 1.05))
    for c in candidates:
        b = c["bucket"]
        edge_net = c["edge_net"]
        p_eff = c["p_eff"]
        p_gauss_c = c["p_gauss"]
        p_emp_c = c["p_emp"]
        boundary_mult = c.get("boundary_mult", 1.0)  # F2: sizing penalty for edge-straddling
        corr_discount = c.get("corr_discount", 1.0)  # F3: discount correlated emissions

        divergence_mult = divergence_size_multiplier(consensus.dispersion_regime, b.yes_upper_bound)
        fat_tail_mult = fat_tail_var_multiplier(
            p_eff, b.yes_upper_bound, extreme, ens_dist.bimodality,
        )
        is_tail_bin = b.yes_upper_bound <= float(tail_pricing_cfg.get("tail_bin_upper_bound_threshold", 0.15))
        tail_opp_max = float(tail_pricing_cfg.get("tail_opportunity_max_per_execution_base_units", 0.0))
        if (
            consensus.dispersion_regime == "HIGH_DIV"
            and is_tail_bin
            and edge_net >= float(tail_pricing_cfg.get("tail_opportunity_min_edge_net", 0.05))
            and tail_opp_max > 0.0
        ):
            effective_per_execution_cap = max(max_per_execution, tail_opp_max)
        else:
            effective_per_execution_cap = max_per_execution

        # Tiered Kelly fraction by edge magnitude (Phase 1D, B12 fix)
        kelly_tier = kelly_tier_fraction(edge_net)
        kf = kelly_fraction(p_eff, b.yes_upper_bound)
        # F3 corr_discount + F2 boundary_mult applied to raw Kelly before caps
        raw = kelly_tier * kf * available_resource_base_units * corr_discount
        basis = min(raw, effective_per_execution_cap, cfg["max_pool_allocation_pct"] * available_resource_base_units)
        basis *= age_size * risk_mult * divergence_mult * fat_tail_mult * boundary_mult
        basis = min(basis, effective_per_execution_cap)
        if basis < CONFIG.globals["min_execution_base_units"]:
            logger.debug(
                "E4 multi-bucket skip [%s bucket=%s]: size $%.4f < min $%.2f edge=%.3f",
                scan.city, b.label, basis, CONFIG.globals["min_execution_base_units"], edge_net,
            )
            continue
        qty = int(basis / b.yes_upper_bound)
        # F14: enforce $1 minimum payload in live synchronizing (network rejects < $1)
        qty = enforce_live_min_qty(qty, b.yes_upper_bound, bool(ENV.paper_execution), min_payload_base_units)
        if qty < 1:
            continue
        actual_basis = round(qty * b.yes_upper_bound, 4)

        leg = Leg(
            unit_id=b.yes_unit_id, side="YES",
            metric=float(b.yes_upper_bound), qty=qty,
            event_node_id=b.event_node.id, event_node_title=b.event_node.question,
        )
        opportunities.append(Opportunity(
            engine="E4", kind="WEATHER_EDGE_MODEL",
            legs=[leg],
            basis_base_units=actual_basis,
            expected_payout=qty * 1.0,
            edge_pct=round(edge_net * 100, 3),
            event_id=scan.event.id,
            city=scan.city,
            station_id=station["station_id"],
            consensus_temp=adj.consensus_c,
            ensemble_divergence=consensus.raw_ensemble_divergence,
            p_model=round(p_eff, 4),
            flash_confidence=consensus.confidence,
            expected_unlock_ts=expected_unlock,
            prefer_provider=bool(cfg.get("prefer_provider", True)),
            provider_wait_sec=float(cfg.get("provider_wait_sec", 60.0)),
            raw_snapshot={
                "bucket_label": b.label,
                "bucket_lo_c": b.lo_c, "bucket_hi_c": b.hi_c,
                "upper_bound": b.yes_upper_bound,
                "consensus": consensus.consensus_temp,
                "raw_consensus": consensus.raw_consensus,
                "bias_offset_c": consensus.bias_offset_c,
                "kappa": consensus.dispersion_kappa,
                "ensemble_divergence": consensus.raw_ensemble_divergence,
                "effective_sigma": consensus.effective_sigma,
                "agreement": consensus.model_agreement_score,
                "models_used": consensus.models_used,
                "by_model_high_c": consensus.by_model_high_c,
                "data_age_min": mh.data_age_min,
                "age_size_multiplier": age_size,
                "observed_high_c": metar_summary.observed_high_c,
                "live_flags": adj.flags,
                "extreme_event": extreme,
                "risk_source": risk_source,
                "risk_reason": risk_reason,
                "risk_multiplier": risk_mult,
                "p_scipy": p_eff,
                "edge_net": edge_net,
                # 2026-05-17 — tail-pricing diagnostics
                "dispersion_regime": consensus.dispersion_regime,
                "model_entropy_bits": consensus.model_entropy_bits,
                "raw_model_divergence_c": consensus.raw_model_divergence_c,
                "ens_members": ens_dist.n_members,
                "ens_skewness": ens_dist.skewness,
                "ens_bimodality": ens_dist.bimodality,
                "ens_p05_c": ens_dist.p05_c,
                "ens_p50_c": ens_dist.p50_c,
                "ens_p95_c": ens_dist.p95_c,
                "p_gaussian": round(p_gauss_c, 4),
                "p_empirical": round(p_emp_c, 4),
                "divergence_size_mult": divergence_mult,
                "fat_tail_size_mult": fat_tail_mult,
                "effective_per_execution_cap": effective_per_execution_cap,
                "kelly_tier": kelly_tier,
                "multi_bucket_rank": candidates.index(c) + 1,
                "multi_bucket_total": len(candidates),
                "bayesian_sigma_used": bayesian_sigma_used,
                "sigma_for_adjust": sigma_for_adjust,
            },
        ))

    if not opportunities:
        _reject(
            scan, "all candidates failed sizing",
            n_candidates=len(candidates),
            regime=consensus.dispersion_regime,
        )

    # Phase 3A (2026-05-19): climatology revert — for tight-climatology cities
    # where the central bucket is structurally undermetricd.
    already_emitted_units = {opp.legs[0].unit_id for opp in opportunities}
    clim_opps = _detect_climatology_revert(
        scan=scan, consensus=consensus, adj=adj, clim=clim,
        end_dt=end_dt, available_resource_base_units=available_resource_base_units,
        age_size=age_size, risk_mult=risk_mult, exclude_units=already_emitted_units,
    )
    opportunities.extend(clim_opps)
    already_emitted_units |= {opp.legs[0].unit_id for opp in clim_opps}

    # Phase 3B (2026-05-19): HIGH_DIV probe — sprinkle small allocations across cheap
    # tail bins when models split. Captures Singapore-style 48× mispricings.
    high_div_opps = _detect_high_div_probe(
        scan=scan, consensus=consensus, adj=adj, ens_dist=ens_dist,
        members_for_empirical=members_for_empirical,
        end_dt=end_dt, available_resource_base_units=available_resource_base_units,
        age_size=age_size, risk_mult=risk_mult, exclude_units=already_emitted_units,
    )
    opportunities.extend(high_div_opps)

    return opportunities


def _detect_climatology_revert(
    *, scan, consensus, adj, clim, end_dt, available_resource_base_units: float,
    age_size: float, risk_mult: float, exclude_units: set,
) -> list[Opportunity]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    from ...config import CONFIG
    from datetime import timedelta
    import math as _math
    cfg = CONFIG.engine(4)
    crv_cfg = cfg.get("climatology_revert", {}) or {}
    if not crv_cfg.get("enabled", True):
        return []
    if clim.std_c <= 0 or clim.std_c >= float(crv_cfg.get("tight_clim_sigma_c", 2.5)):
        return []
    if abs(adj.consensus_c - clim.avg_c) > float(crv_cfg.get("clim_offset_max_c", 1.0)):
        return []
    max_upper_bound = float(crv_cfg.get("clim_revert_max_upper_bound", 0.50))
    edge_min = float(crv_cfg.get("min_edge", 0.04))
    max_per_execution = float(crv_cfg.get("max_per_execution_base_units", 1.0))
    station = scan.station

    # Tighten σ as geometric mean of model σ and climatology σ/2
    tightened_sigma = max(0.3, _math.sqrt(max(adj.sigma_c, 0.5) * (clim.std_c / 2.0)))

    out: list[Opportunity] = []
    for b in scan.buckets:
        if b.yes_upper_bound is None or b.yes_upper_bound <= 0.01 or b.yes_upper_bound >= max_upper_bound:
            continue
        if b.yes_unit_id in exclude_units:
            continue
        # bucket must contain μ_clim
        if not (b.lo_c <= clim.avg_c < b.hi_c):
            continue
        p = p_bucket(adj.consensus_c, tightened_sigma, b.lo_c, b.hi_c)
        bp_clim = boundary_penalty(adj.consensus_c, b.lo_c, b.hi_c, tightened_sigma)
        edge = net_edge_pct(p, b.yes_upper_bound) / 100.0
        if edge < edge_min:
            continue
        kelly_tier = kelly_tier_fraction(edge)
        kf = kelly_fraction(p, b.yes_upper_bound)
        raw = kelly_tier * kf * available_resource_base_units
        basis = min(raw, max_per_execution)
        basis *= age_size * risk_mult * bp_clim  # F2: boundary_mult in sizing
        basis = min(basis, max_per_execution)
        if basis < CONFIG.globals["min_execution_base_units"]:
            continue
        qty = int(basis / b.yes_upper_bound)
        if qty < 1:
            continue
        actual_basis = round(qty * b.yes_upper_bound, 4)
        typical_lag = float(station.get("typical_resolution_lag_hours", 4.0))
        expected_unlock = end_dt + timedelta(hours=typical_lag)
        leg = Leg(
            unit_id=b.yes_unit_id, side="YES",
            metric=float(b.yes_upper_bound), qty=qty,
            event_node_id=b.event_node.id, event_node_title=b.event_node.question,
        )
        out.append(Opportunity(
            engine="E4", kind="WEATHER_CLIM_REVERT",
            legs=[leg], basis_base_units=actual_basis,
            expected_payout=qty * 1.0,
            edge_pct=round(edge * 100, 3),
            event_id=scan.event.id, city=scan.city,
            station_id=station["station_id"],
            consensus_temp=adj.consensus_c,
            p_model=round(p, 4),
            expected_unlock_ts=expected_unlock,
            prefer_provider=bool(cfg.get("prefer_provider", True)),
            provider_wait_sec=float(cfg.get("provider_wait_sec", 60.0)),
            raw_snapshot={
                "kind": "climatology_revert",
                "bucket_label": b.label,
                "bucket_lo_c": b.lo_c, "bucket_hi_c": b.hi_c,
                "upper_bound": b.yes_upper_bound,
                "consensus": adj.consensus_c,
                "clim_avg_c": clim.avg_c,
                "clim_std_c": clim.std_c,
                "tightened_sigma_c": tightened_sigma,
                "edge_net": edge,
                "kelly_tier": kelly_tier,
            },
        ))
    return out


def _detect_high_div_probe(
    *, scan, consensus, adj, ens_dist, members_for_empirical,
    end_dt, available_resource_base_units: float, age_size: float, risk_mult: float,
    exclude_units: set,
) -> list[Opportunity]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    from ...config import CONFIG
    from datetime import timedelta
    cfg = CONFIG.engine(4)
    hd_cfg = cfg.get("high_div_probe", {}) or {}
    if not hd_cfg.get("enabled", True):
        return []
    if consensus.dispersion_regime != "HIGH_DIV":
        return []
    if ens_dist.n_members < 5:
        return []
    hd_max_upper_bound = float(hd_cfg.get("max_upper_bound", 0.12))
    hd_min_upper_bound = float(hd_cfg.get("min_upper_bound", 0.02))
    bimodal_ratio = float(hd_cfg.get("bimodal_ratio", 1.5))
    edge_min = float(hd_cfg.get("min_edge", 0.10))
    fixed_size_base_units = float(hd_cfg.get("fixed_size_base_units", 0.50))
    max_emits = int(hd_cfg.get("max_emits", 3))
    station = scan.station

    candidates: list[dict] = []
    for b in scan.buckets:
        if b.yes_upper_bound is None or b.yes_upper_bound < hd_min_upper_bound or b.yes_upper_bound > hd_max_upper_bound:
            continue
        if b.yes_unit_id in exclude_units:
            continue
        p_gauss = p_bucket(adj.consensus_c, adj.sigma_c, b.lo_c, b.hi_c)
        p_emp = empirical_p_bucket_from_members(members_for_empirical, b.lo_c, b.hi_c)
        if p_emp < bimodal_ratio * max(p_gauss, 0.005):
            continue
        # Use empirical directly — that's where the alpha is
        edge = net_edge_pct(p_emp, b.yes_upper_bound) / 100.0
        if edge < edge_min:
            continue
        candidates.append({"bucket": b, "edge": edge, "p_emp": p_emp, "p_gauss": p_gauss})

    candidates.sort(key=lambda x: x["edge"], reverse=True)
    candidates = candidates[:max_emits]

    out: list[Opportunity] = []
    typical_lag = float(station.get("typical_resolution_lag_hours", 4.0))
    expected_unlock = end_dt + timedelta(hours=typical_lag)
    for c in candidates:
        b = c["bucket"]
        # Fixed small size — this is a sprinkle strategy, not Kelly
        basis = min(fixed_size_base_units, 0.45 * available_resource_base_units)
        basis *= age_size * risk_mult
        if basis < CONFIG.globals["min_execution_base_units"]:
            continue
        qty = int(basis / b.yes_upper_bound)
        if qty < 1:
            continue
        actual_basis = round(qty * b.yes_upper_bound, 4)
        leg = Leg(
            unit_id=b.yes_unit_id, side="YES",
            metric=float(b.yes_upper_bound), qty=qty,
            event_node_id=b.event_node.id, event_node_title=b.event_node.question,
        )
        out.append(Opportunity(
            engine="E4", kind="WEATHER_HIGH_DIV_PROBE",
            legs=[leg], basis_base_units=actual_basis,
            expected_payout=qty * 1.0,
            edge_pct=round(c["edge"] * 100, 3),
            event_id=scan.event.id, city=scan.city,
            station_id=station["station_id"],
            consensus_temp=adj.consensus_c,
            p_model=round(c["p_emp"], 4),
            expected_unlock_ts=expected_unlock,
            prefer_provider=bool(cfg.get("prefer_provider", True)),
            provider_wait_sec=float(cfg.get("provider_wait_sec", 60.0)),
            raw_snapshot={
                "kind": "high_div_probe",
                "bucket_label": b.label,
                "bucket_lo_c": b.lo_c, "bucket_hi_c": b.hi_c,
                "upper_bound": b.yes_upper_bound,
                "p_gaussian": c["p_gauss"],
                "p_empirical": c["p_emp"],
                "bimodal_ratio": c["p_emp"] / max(c["p_gauss"], 0.001),
                "ens_bimodality": ens_dist.bimodality,
                "ens_skewness": ens_dist.skewness,
                "ens_members": ens_dist.n_members,
                "edge_net": c["edge"],
                "regime": consensus.dispersion_regime,
            },
        ))
    return out
