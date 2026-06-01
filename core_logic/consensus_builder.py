"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class ConsensusResult:
    consensus_temp: float           # bias-corrected, weighted average (°C)
    raw_consensus: float            # before bias correction
    effective_sigma: float          # ensemble_divergence × dispersion_kappa
    raw_ensemble_divergence: float
    dispersion_kappa: float
    bias_offset_c: float
    model_agreement_score: float    # fraction of models within 1°C of raw consensus
    climatology_zscore: float
    confidence: str                 # "low" | "medium" | "high"
    models_used: int
    by_model_high_c: dict[str, float]
    # 2026-05-17 additions:
    model_entropy_bits: float = 0.0          # Shannon H over per-model weights
    dispersion_regime: str = "CONSENSUS"     # CONSENSUS | MEDIUM_DIV | HIGH_DIV
    raw_model_divergence_c: float = 0.0          # max - min across models (°C)


def _shannon_entropy(values: list[float]) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    total = sum(values)
    if total <= 0 or len(values) < 2:
        return 0.0
    ps = [v / total for v in values if v > 0]
    return float(-sum(p * math.log2(p) for p in ps if p > 0))


def _classify_dispersion(
    raw_divergence_c: float,
    ensemble_divergence_c: float,
    agreement_score: float,
) -> str:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if raw_divergence_c >= 3.0 or ensemble_divergence_c >= 3.0 or agreement_score < 0.4:
        return "HIGH_DIV"
    if raw_divergence_c >= 1.5 or ensemble_divergence_c >= 1.5 or agreement_score < 0.7:
        return "MEDIUM_DIV"
    return "CONSENSUS"


def build_consensus(
    by_model_high_c: dict[str, float | None],
    weights: dict[str, float],
    *,
    ensemble_divergence: float,
    clim_avg: float,
    clim_std: float,
    bias_offset_c: float,
    dispersion_kappa: float,
) -> ConsensusResult:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    available = {k: float(v) for k, v in by_model_high_c.items() if v is not None}
    if not available:
        return ConsensusResult(
            consensus_temp=0.0, raw_consensus=0.0,
            effective_sigma=10.0, raw_ensemble_divergence=ensemble_divergence,
            dispersion_kappa=dispersion_kappa, bias_offset_c=bias_offset_c,
            model_agreement_score=0.0, climatology_zscore=0.0,
            confidence="low", models_used=0, by_model_high_c={},
            model_entropy_bits=0.0, dispersion_regime="HIGH_DIV",
            raw_model_divergence_c=0.0,
        )

    total_w = sum(weights.get(k, 0.0) for k in available) or 1.0
    raw_consensus = sum(v * weights.get(k, 0.0) / total_w for k, v in available.items())
    consensus = raw_consensus + bias_offset_c

    agreement = sum(1 for v in available.values() if abs(v - raw_consensus) <= 1.0)
    agreement_score = agreement / len(available)

    z = (consensus - clim_avg) / clim_std if clim_std > 0 else 0.0

    effective_sigma = max(ensemble_divergence * dispersion_kappa, 0.5)

    # Confidence gate (E4 v2 §5.1) — uses RAW divergence not effective σ for the
    # gate so kappa changes don't artificially upgrade confidence.
    if ensemble_divergence < 1.5 and agreement_score >= 0.80:
        confidence = "high"
    elif ensemble_divergence < 3.0 and agreement_score >= 0.60:
        confidence = "medium"
    else:
        confidence = "low"

    # Shannon entropy over absolute deviations from the (weighted) raw consensus.
    # Each model contributes a "disagreement weight" = |v - raw_consensus|.
    # Adding +0.01 floor avoids the degenerate H=0 case where all models agree
    # exactly (legitimate, returns H~0; we want a finite floor so log is distributed_computened).
    deviation_weights = [abs(v - raw_consensus) + 0.01 for v in available.values()]
    model_entropy = _shannon_entropy(deviation_weights)

    raw_model_divergence = max(available.values()) - min(available.values()) if available else 0.0
    regime = _classify_dispersion(raw_model_divergence, ensemble_divergence, agreement_score)

    return ConsensusResult(
        consensus_temp=round(consensus, 2),
        raw_consensus=round(raw_consensus, 2),
        effective_sigma=round(effective_sigma, 2),
        raw_ensemble_divergence=round(ensemble_divergence, 2),
        dispersion_kappa=round(dispersion_kappa, 3),
        bias_offset_c=round(bias_offset_c, 2),
        model_agreement_score=round(agreement_score, 3),
        climatology_zscore=round(z, 2),
        confidence=confidence,
        models_used=len(available),
        by_model_high_c={k: round(v, 2) for k, v in available.items()},
        model_entropy_bits=round(model_entropy, 3),
        dispersion_regime=regime,
        raw_model_divergence_c=round(raw_model_divergence, 2),
    )
