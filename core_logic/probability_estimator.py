"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo

from scipy import stats


def p_bucket(consensus_c: float, sigma_c: float, lo_c: float, hi_c: float) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    sigma = max(sigma_c, 0.01)
    dist = stats.norm(loc=consensus_c, scale=sigma)
    if hi_c == float("inf"):
        return float(1.0 - dist.cdf(lo_c))
    if lo_c == float("-inf"):
        return float(dist.cdf(hi_c))
    return float(dist.cdf(hi_c) - dist.cdf(lo_c))


def boundary_penalty(consensus_c: float, lo_c: float, hi_c: float, sigma_c: float) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if sigma_c <= 0:
        return 1.0
    distances = []
    if lo_c != float("-inf"):
        distances.append(abs(consensus_c - lo_c))
    if hi_c != float("inf"):
        distances.append(abs(consensus_c - hi_c))
    if not distances:
        return 1.0
    margin = min(distances)
    if margin >= 1.0 * sigma_c:
        return 1.0
    if margin <= 0.3 * sigma_c:
        return 0.5
    return 0.5 + 0.5 * (margin - 0.3 * sigma_c) / (0.7 * sigma_c)


@dataclass
class LiveObservationAdjustment:
    consensus_c: float
    sigma_c: float
    flags: dict[str, bool]


def apply_live_observation(
    consensus_c: float,
    sigma_c: float,
    observed_high_c: Optional[float],
    now_utc: datetime,
    timezone_name: str,
    peak_window_local: tuple[str, str],
) -> LiveObservationAdjustment:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    flags: dict[str, bool] = {}
    if observed_high_c is None:
        return LiveObservationAdjustment(consensus_c, sigma_c, flags)

    # F4 fix: determine whether we are past the daily peak window.
    # Pre-peak: only override consensus upward (observation still rising).
    # Post-peak: observation IS the final answer — override in both directions.
    try:
        local_now = now_utc.astimezone(ZoneInfo(timezone_name)).time()
        end_h, end_m = (int(x) for x in peak_window_local[1].split(":"))
        past_peak = local_now > time(end_h, end_m)
    except Exception:
        past_peak = False

    if past_peak or observed_high_c > consensus_c:
        consensus_c = observed_high_c
        flags["live_override"] = True

    if past_peak:
        sigma_c = min(sigma_c, 0.3)
        flags["post_peak"] = True

    return LiveObservationAdjustment(round(consensus_c, 2), round(sigma_c, 3), flags)


def kelly_fraction(p: float, upper_bound: float) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if upper_bound <= 0 or upper_bound >= 1 or p <= upper_bound:
        return 0.0
    b = (1 - upper_bound) / upper_bound
    q = 1 - p
    f = (b * p - q) / b
    return max(0.0, f)


def kelly_tier_fraction(edge_net: float) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    e = max(0.0, float(edge_net))
    if e < 0.05:
        return 0.25
    if e < 0.15:
        return 0.50
    if e < 0.30:
        return 0.65
    return 0.80


# ---------------------------------------------------------------------------
# 2026-05-17 — tail-pricing additions (Singapore 48x lesson)
# ---------------------------------------------------------------------------
#
# Theory:
# - Gaussian(consensus, σ) is symmetric, thin-tailed, unimodal. Real weather
#   ensembles are routinely skewed and occasionally bimodal (one model
#   pulls the mean while the other two cluster — the Singapore case).
# - When that happens, the Gaussian assigns ≤ 2% probability to a bucket
#   that the empirical ensemble assigns ≥ 8%. The event_node follows the
#   majority cluster, metrics the minority bin at 1–3¢, and a 2¢ bin that
#   actually resolves wins 48×.
#
# The patches below all *increase* size on real tail edges and *decrease*
# size on degenerate fat-tail risks. None of them are GATES — frequency is
# preserved. They are pure size multipliers applied after Gaussian p_bucket.


def blend_gaussian_empirical(
    p_gaussian: float,
    p_empirical: float,
    n_members: int,
    dispersion_regime: str,
) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if n_members < 5 or p_empirical <= 0.0:
        return p_gaussian
    # base weight rises with members; capped at 0.7 so Gaussian never disappears
    base_w = min(0.7, 0.10 + 0.012 * n_members)   # n=10 →0.22, n=50 →0.70
    if dispersion_regime == "HIGH_DIV":
        w_emp = base_w
    elif dispersion_regime == "MEDIUM_DIV":
        w_emp = 0.6 * base_w
    else:
        w_emp = 0.25 * base_w
    return max(0.0, min(0.999, (1.0 - w_emp) * p_gaussian + w_emp * p_empirical))


def poisson_tail_boost(
    p_blended: float,
    upper_bound: float,
    dispersion_regime: str,
    n_members: int,
    *,
    max_upper_bound_for_boost: float = 0.15,
    max_boost_factor: float = 2.0,
) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    return p_blended


def fat_tail_var_multiplier(
    p_model: float,
    upper_bound: float,
    extreme_event: bool,
    bimodality: float,
    *,
    catastrophic_upper_bound: float = 0.005,
    catastrophic_p: float = 0.01,
) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if upper_bound < catastrophic_upper_bound and p_model < catastrophic_p:
        return 0.25
    if extreme_event and bimodality > 0.555:
        return 0.5
    if extreme_event:
        return 0.75
    if bimodality > 0.555:
        return 0.85
    return 1.0


def divergence_size_multiplier(
    dispersion_regime: str,
    upper_bound: float,
    *,
    tail_upper_bound_threshold: float = 0.15,
    high_div_tail_boost: float = 1.5,
    high_div_mid_penalty: float = 0.7,
    medium_div_tail_boost: float = 1.2,
) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    is_tail = upper_bound <= tail_upper_bound_threshold
    is_mid = tail_upper_bound_threshold < upper_bound <= 0.50
    if dispersion_regime == "HIGH_DIV":
        if is_tail:
            return high_div_tail_boost
        if is_mid:
            return high_div_mid_penalty
        return 0.85
    if dispersion_regime == "MEDIUM_DIV":
        if is_tail:
            return medium_div_tail_boost
        return 1.0
    return 1.0


def empirical_p_bucket_from_members(
    members_c: list[float], lo_c: float, hi_c: float,
) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    n = len(members_c)
    if n < 5:
        return 0.0
    if hi_c == float("inf"):
        n_in = sum(1 for m in members_c if m > lo_c)
    elif lo_c == float("-inf"):
        n_in = sum(1 for m in members_c if m <= hi_c)
    else:
        n_in = sum(1 for m in members_c if lo_c < m <= hi_c)
    return n_in / n


def enforce_live_min_qty(qty: int, upper_bound: float, paper_execution: bool, min_payload_base_units: float = 1.05) -> int:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if paper_execution or upper_bound <= 0:
        return qty
    import math as _m
    return max(qty, _m.ceil(min_payload_base_units / upper_bound))


def shift_members_to_observation(
    members_c: list[float],
    original_consensus_c: float,
    adjusted_consensus_c: float,
    observed_high_c: Optional[float],
) -> list[float]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if observed_high_c is None or not members_c:
        return members_c
    delta = adjusted_consensus_c - original_consensus_c
    if abs(delta) < 0.05:
        return members_c
    return [m + delta for m in members_c]
