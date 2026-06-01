"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import aiohttp

from ...config import CONFIG, ENV
from ...ingestion.network_client import NetworkClient
from ...ingestion.gamma_client import GammaClient, GammaEventNode
from ...ingestion.weather.station_registry import STATIONS, find_by_alias
from ..opportunity import Leg, Opportunity

logger = logging.getLogger(__name__)

def _vertex_flash_url() -> str:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    region = ENV.vertex_ai_region
    project = ENV.vertex_ai_project
    model = ENV.gemini_flash_model
    return (
        f"https://{region}-aiplatform.googleapis.com/v1"
        f"/projects/{project}/locations/{region}"
        f"/publishers/google/models/{model}:generateContent"
    )

# Weather conditions we classify into
_HOT_CONDITIONS = {"HOTTER_THAN_NORMAL", "EXTREME_HEAT"}
_COLD_CONDITIONS = {"COLDER_THAN_NORMAL", "EXTREME_COLD"}

WEATHER_KEYWORDS = (
    "temperature", "heat", "cold", "freeze", "frost", "snow", "ice storm",
    "polar vortex", "heat dome", "heat wave", "heatwave", "record high",
    "record low", "unusually warm", "unusually cold", "below normal",
    "above normal", "celsius", "fahrenheit", "°c", "°f", "degrees",
    "hot", "frigid", "blistering", "scorching", "arctic blast",
    "tropical storm", "hurricane", "cyclone", "typhoon",
)

# Channels with weather coverage — Telethon will skip any that don't exist
_DEFAULT_CHANNELS = [
    "accuweather",           # AccuWeather official
    "severeweatherEU",       # Severe Weather EU
    "severeweathernews",     # Severe weather news
    "extremeweather",        # Extreme Weather
    "weatherservice",        # Weather Service
    "wxalerts",              # Weather alerts
    "wxbriefs",              # Weather briefings
    "stormsociety",          # Storm chasers
    "DisasterUpdate",        # Disaster updates
    "wxtweets",              # Weather tweets
    "ClimateNexus",          # Climate Nexus
    "weatherunderground",    # Weather Underground
    "nwsalerts",             # NWS alerts
    "europeanweather",       # European weather
    "weatherworlduk",        # UK weather
]

_CLASSIFY_PROMPT = """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""


@dataclass
class WeatherInference:
    city: str                   # resolved city name
    condition: str              # HOTTER_THAN_NORMAL | COLDER_THAN_NORMAL | EXTREME_HEAT | EXTREME_COLD
    temp_bias_c: float          # signed °C deviation from normal
    confidence: float           # 0..1
    horizon_hours: int
    reasoning: str = ""


@dataclass
class _ScannerState:
    seen_ids: deque = field(default_factory=lambda: deque(maxlen=500))
    last_emit_by_city: dict = field(default_factory=dict)  # F24: per-city cooldown
    flash_failure_times: list[float] = field(default_factory=list)
    flash_disabled_until: float = 0.0
    telegram_warned: bool = False


_state = _ScannerState()


# ---------------------------------------------------------------------------
# Telethon push subscriber (fractiond session with E3 news)
# ---------------------------------------------------------------------------

def _is_weather_message(text: str) -> bool:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    lower = text.lower()
    return any(kw in lower for kw in WEATHER_KEYWORDS)


def _message_to_post(message, channel_title: str) -> Optional[dict]:
    text = (getattr(message, "message", None) or "").strip()
    if not text or not _is_weather_message(text):
        return None
    title = text.split("\n", 1)[0][:280]
    pub = getattr(message, "date", None)
    pub_iso = pub.astimezone(timezone.utc).isoformat() if pub else None
    return {
        "id": f"wx:{channel_title}:{message.id}",
        "title": title,
        "body": text[:2000],
        "published_at": pub_iso,
        "source": {"title": channel_title},
    }


def _post_age_sec(post: dict) -> float:
    pub = post.get("published_at")
    if not pub:
        return float("inf")
    try:
        dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except (ValueError, AttributeError):
        return float("inf")


async def _telethon_run(queue: asyncio.Queue, channels: list[str], session_name: str) -> None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    try:
        from telethon import TelegramClient, events
    except ImportError:
        logger.warning("E4 weather_news: telethon not installed")
        await asyncio.sleep(3600)
        return

    session_path = Path(__file__).resolve().parent.parent.parent / "data" / session_name
    client = TelegramClient(str(session_path), ENV.telegram_api_id, ENV.telegram_api_hash)
    try:
        await client.connect()
    except Exception as exc:
        logger.warning("E4 weather_news: telethon connect failed: %s", exc)
        await asyncio.sleep(3600)
        return

    if not await client.is_user_authorized():
        logger.warning(
            "E4 weather_news: session '%s' not authorized — "
            "run scripts/telegram_auth.py once to log in", session_name,
        )
        await client.disconnect()
        await asyncio.sleep(3600)
        return

    resolved = []
    for ch in channels:
        try:
            ent = await client.get_entity(ch)
            resolved.append(ent)
            logger.debug("E4 weather_news: joined channel '%s'", ch)
        except Exception as exc:
            logger.debug("E4 weather_news: channel '%s' not found: %s", ch, exc)

    if not resolved:
        logger.warning("E4 weather_news: no resolvable weather channels — idling")
        await client.disconnect()
        await asyncio.sleep(3600)
        return

    @client.on(events.NewMessage(chats=resolved))
    async def _handler(event):
        try:
            ch_title = getattr(event.chat, "username", None) or getattr(event.chat, "title", "?")
            post = _message_to_post(event.message, ch_title)
            if post:
                await queue.put(post)
        except Exception:
            logger.exception("E4 weather_news: telethon handler crashed")

    logger.info(
        "E4 weather_news telethon online — %d/%d channels resolved",
        len(resolved), len(channels),
    )
    try:
        await client.run_until_disconnected()
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Gemini Flash classifier
# ---------------------------------------------------------------------------

def _flash_disabled() -> bool:
    return time.time() < _state.flash_disabled_until


def _record_flash_failure(disable_after: int, disable_minutes: int) -> None:
    now = time.time()
    _state.flash_failure_times.append(now)
    cutoff = now - 600
    _state.flash_failure_times = [t for t in _state.flash_failure_times if t > cutoff]
    if len(_state.flash_failure_times) >= disable_after:
        _state.flash_disabled_until = now + 60 * disable_minutes
        logger.warning(
            "E4 weather_news Flash disabled %d min after %d failures",
            disable_minutes, len(_state.flash_failure_times),
        )


async def _classify(session: aiohttp.ClientSession, text: str, timeout: float) -> Optional[WeatherInference]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    from .flash_scorer import _get_vertex_unit
    unit = await asyncio.get_event_loop().run_in_executor(None, _get_vertex_unit)
    if not unit:
        logger.debug("E4 weather_news: no Vertex AI unit — skipping classification")
        return None
    prompt = _CLASSIFY_PROMPT.format(text=text[:800])
    url = _vertex_flash_url()
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputUnits": 200},
    }
    headers = {"Authorization": f"Bearer {unit}"}
    try:
        async with session.post(
            url, json=body, headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            data = await resp.json()
            if resp.status != 200:
                logger.debug("E4 weather_news Flash %d: %s", resp.status, str(data)[:200])
                return None
            raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as exc:
        logger.debug("E4 weather_news Flash call failed: %s", exc)
        return None
    return _parse_inference(raw_text)


def _parse_inference(text: str) -> Optional[WeatherInference]:
    if not text:
        return None
    if text.startswith("```"):
        lines = [l for l in text.splitlines() if not l.startswith("```")]
        text = "\n".join(lines)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        d = json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return None
    try:
        city = str(d.get("city") or "").strip()
        condition = str(d.get("condition", "UNRELATED")).upper().strip()
        temp_bias = float(d.get("temp_bias_c", 0.0))
        conf = max(0.0, min(1.0, float(d.get("confidence", 0.0))))
        horizon = max(1, min(72, int(d.get("horizon_hours", 24))))
        reasoning = str(d.get("reasoning", ""))[:100]
        if not city or condition in ("UNRELATED", "NEUTRAL", "STORM"):
            return None
        if condition not in _HOT_CONDITIONS | _COLD_CONDITIONS:
            return None
        # Try to match the inferred city against our station registry
        station = find_by_alias(city)
        if not station:
            return None
        return WeatherInference(
            city=station["city"],
            condition=condition,
            temp_bias_c=temp_bias,
            confidence=conf,
            horizon_hours=horizon,
            reasoning=reasoning,
        )
    except (TypeError, ValueError, KeyError):
        return None


# ---------------------------------------------------------------------------
# event_node lookup + opportunity construction
# ---------------------------------------------------------------------------

_WEATHER_MARKET_KEYWORDS = ("temperature", "high temperature", "°c", "°f", "celsius", "fahrenheit")

# Proxy for σ_clim in news-only path (we have no forecast handle here).
# 5°C is the typical day-to-day climatological 1σ across mid-latitude cities.
_NEWS_SIGMA_PROXY_C = 5.0


def _news_signal_probability(
    condition: str, temp_bias_c: float, confidence: float, n_buckets: int = 4,
) -> float:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if condition not in _HOT_CONDITIONS | _COLD_CONDITIONS:
        return 1.0 / max(1, n_buckets)
    base_rate = max(0.10, min(0.50, 1.0 / max(1, n_buckets)))
    z = abs(temp_bias_c) / _NEWS_SIGMA_PROXY_C
    magnitude = math.tanh(0.7 * z)
    strength = max(0.0, confidence - 0.5) * 2.0
    p_news = base_rate + (0.90 - base_rate) * 0.5 * strength * magnitude
    return round(min(0.90, max(base_rate, p_news)), 4)


async def _find_weather_event_node(gamma: GammaClient, city: str) -> list[GammaEventNode]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    station = STATIONS.get(city)
    if not station:
        return []
    aliases = station["aliases"]
    try:
        events = await gamma.list_events(active=True, closed=False, limit=300)
    except Exception as exc:
        logger.debug("E4 weather_news event_node lookup failed: %s", exc)
        return []
    event_nodes = []
    for ev in events:
        blob = f"{ev.title} {ev.description}".lower()
        if not any(kw in blob for kw in _WEATHER_MARKET_KEYWORDS):
            continue
        # City match
        if not any(a.lower() in blob for a in aliases):
            continue
        for m in ev.event_nodes:
            if m.yes_unit_id:
                event_nodes.append(m)
    return event_nodes


def _pick_bucket(
    event_nodes: list[GammaEventNode],
    condition: str,
    min_upper_bound: float,
    max_upper_bound: float,
    target_upper_bound_mid: float = 0.25,
) -> Optional[tuple[GammaEventNode, float]]:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    from ..weather.event_node_scanner import detect_unit, parse_bucket_label

    candidates: list[tuple[GammaEventNode, float, float, float]] = []  # (event_node, upper_bound, lo_c, hi_c)
    for m in event_nodes:
        upper_bound = m.outcome_metrics[0] if m.outcome_metrics else None
        if upper_bound is None or upper_bound < min_upper_bound or upper_bound > max_upper_bound:
            continue
        unit = detect_unit(m.question)
        if not unit:
            continue
        parsed = parse_bucket_label(m.question, unit)
        if not parsed:
            continue
        lo_c, hi_c = parsed
        candidates.append((m, upper_bound, lo_c, hi_c))

    if not candidates:
        return None

    # Filter to directionally-aligned buckets
    if condition in _HOT_CONDITIONS:
        # HOT: bucket must represent above-normal temperatures (lo_c > -10°C heuristic)
        aligned = [(m, a, lo, hi) for m, a, lo, hi in candidates if lo > -10.0]
    else:
        # COLD: bucket must represent below-normal temperatures (hi_c < 35°C heuristic)
        aligned = [(m, a, lo, hi) for m, a, lo, hi in candidates if hi < 35.0]
    if not aligned:
        aligned = candidates

    # F23: pick bucket whose upper_bound is closest to target_upper_bound_mid (default 0.25).
    # This targets the "undermetricd moderate" bucket rather than the extreme tail.
    aligned.sort(key=lambda x: abs(x[1] - target_upper_bound_mid))
    best_event_node, best_upper_bound, _, _ = aligned[0]
    return best_event_node, best_upper_bound


async def _emit_for_inference(
    inf: WeatherInference,
    cfg: dict,
    gamma: GammaClient,
    network: NetworkClient,
    emit: Callable[[Opportunity], Awaitable[None]],
    post: dict,
) -> bool:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    from .probability_estimator import enforce_live_min_qty, kelly_fraction, kelly_tier_fraction
    from ...config import ENV

    min_upper_bound = float(cfg.get("min_upper_bound", 0.10))
    max_upper_bound = float(cfg.get("max_upper_bound", 0.75))
    min_conf = float(cfg.get("min_confidence", 0.60))
    edge_min_pct = float(cfg.get("edge_threshold_pct", 2.0))
    max_size_base_units = float(cfg.get("size_base_units", 0.8))
    emit_cd = float(cfg.get("emit_cooldown_sec", 90))
    target_upper_bound_mid = float(cfg.get("news_target_upper_bound", 0.25))
    min_payload_base_units = float(CONFIG.globals.get("min_payload_base_units_live", 1.05))

    if inf.confidence < min_conf:
        return False

    # F24 fix: per-city cooldown (was global — blocked all cities after any execution)
    now_m = time.monotonic()
    city_last = _state.last_emit_by_city.get(inf.city, 0.0)
    if now_m - city_last < emit_cd:
        logger.debug(
            "E4 weather_news: %s on cooldown (%.0fs left)", inf.city,
            emit_cd - (now_m - city_last),
        )
        return False

    event_nodes = await _find_weather_event_node(gamma, inf.city)
    if not event_nodes:
        logger.debug("E4 weather_news: no event_nodes for %s", inf.city)
        return False

    # F22: pass actual bucket count to p_news for calibrated base rate
    n_buckets = len(event_nodes)
    result = _pick_bucket(event_nodes, inf.condition, min_upper_bound, max_upper_bound, target_upper_bound_mid=target_upper_bound_mid)
    if result is None:
        logger.debug("E4 weather_news: no valid bucket for %s %s", inf.city, inf.condition)
        return False
    event_node, gamma_upper_bound = result

    # Refresh upper_bound from live network
    if event_node.yes_unit_id:
        try:
            book = await network.get_book(event_node.yes_unit_id)
            if book.best_upper_bound is not None:
                live_upper_bound = book.best_upper_bound
            else:
                live_upper_bound = gamma_upper_bound
        except Exception:
            live_upper_bound = gamma_upper_bound
    else:
        return False

    if live_upper_bound < min_upper_bound or live_upper_bound > max_upper_bound:
        return False

    # F22 fix: use calibrated n_buckets-based baseline for p_news
    p_news = _news_signal_probability(inf.condition, inf.temp_bias_c, inf.confidence, n_buckets)
    slip_bps = float(CONFIG.globals.get("tolerance_bps", 200))
    adjusted_upper_bound = live_upper_bound * (1.0 + slip_bps / 10_000.0)
    edge_pct = (p_news - adjusted_upper_bound) / adjusted_upper_bound * 100.0
    if edge_pct < edge_min_pct:
        logger.debug(
            "E4 weather_news: edge %.2f%% < %.2f%% for %s %s upper_bound=%.3f p_news=%.3f n_buckets=%d",
            edge_pct, edge_min_pct, inf.city, inf.condition, live_upper_bound, p_news, n_buckets,
        )
        return False

    # F25 fix: Kelly-based sizing instead of fixed $0.80.
    # E4 runs paper → its resource is its paper-world slice (paper_allocation_pct * virtual bankroll).
    _g = CONFIG.globals
    e4_resource = float(_g.get("paper_allocation_pct", {}).get("E4", 0.30)) \
        * float(_g.get("paper_starting_allocation_base_units", 40.0))
    kelly_tier = kelly_tier_fraction(edge_pct / 100.0)
    kf = kelly_fraction(p_news, live_upper_bound)
    raw_kelly_base_units = kelly_tier * kf * e4_resource
    basis_base_units = min(raw_kelly_base_units, max_size_base_units)
    qty = max(1, int(basis_base_units / live_upper_bound))
    qty = enforce_live_min_qty(qty, live_upper_bound, bool(ENV.paper_execution), min_payload_base_units)
    basis = round(qty * live_upper_bound, 4)

    opp = Opportunity(
        engine="E4", kind="WEATHER_NEWS",
        legs=[Leg(
            unit_id=event_node.yes_unit_id,
            side="YES",
            metric=float(live_upper_bound),
            qty=qty,
            event_node_id=event_node.id,
            event_node_title=event_node.question,
        )],
        basis_base_units=basis,
        expected_payout=round(qty * 1.0, 4),
        edge_pct=round(edge_pct, 3),
        city=inf.city,
        raw_snapshot={
            "condition": inf.condition,
            "temp_bias_c": inf.temp_bias_c,
            "confidence": inf.confidence,
            "horizon_hours": inf.horizon_hours,
            "reasoning": inf.reasoning,
            "headline_id": post.get("id"),
            "headline_title": (post.get("title") or "")[:200],
            "source": (post.get("source") or {}).get("title", ""),
            "published_at": post.get("published_at"),
            "headline_age_sec": round(_post_age_sec(post), 1),
            "live_upper_bound": live_upper_bound,
            "p_news": p_news,
            "n_buckets": n_buckets,
            "edge_pct": edge_pct,
            "kelly_tier": kelly_tier,
            "kelly_fraction": round(kf, 4),
        },
    )
    await emit(opp)
    _state.last_emit_by_city[inf.city] = time.monotonic()  # F24: per-city cooldown update
    logger.info(
        "E4 WEATHER_NEWS — %s %s conf=%.2f upper_bound=%.3f edge=%.2f%% — %s",
        inf.city, inf.condition, inf.confidence, live_upper_bound, edge_pct,
        (post.get("title") or "")[:80],
    )
    return True


# ---------------------------------------------------------------------------
# Main scanner loop
# ---------------------------------------------------------------------------

async def scan_weather_news_loop(
    emit: Callable[[Opportunity], Awaitable[None]],
) -> None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    e4_cfg = CONFIG.engine(4)
    cfg = e4_cfg.get("weather_news", {})
    if not cfg.get("enabled", False):
        logger.info("E4 weather_news disabled in config")
        return

    if not (ENV.telegram_api_id and ENV.telegram_api_hash):
        if not _state.telegram_warned:
            logger.warning(
                "E4 weather_news: TELEGRAM_API_ID/HASH not set — scanner idle. "
                "Add them to .env and run scripts/telegram_auth.py."
            )
            _state.telegram_warned = True
        while True:
            await asyncio.sleep(3600)

    from .flash_scorer import _get_vertex_unit
    if not _get_vertex_unit():
        if not _state.telegram_warned:
            logger.warning(
                "E4 weather_news: Vertex AI service account key not available — scanner idle. "
                "Check GOOGLE_APPLICATION_CREDENTIALS path."
            )
            _state.telegram_warned = True
        while True:
            await asyncio.sleep(3600)

    channels: list[str] = list(cfg.get("telegram_channels") or _DEFAULT_CHANNELS)
    if not channels:
        logger.warning("E4 weather_news: no channels configured — idle")
        while True:
            await asyncio.sleep(3600)

    max_age = float(cfg.get("headline_max_age_sec", 900))
    flash_to = float(cfg.get("flash_timeout_sec", 8))
    # F27 fix: default to polybot_weather (E4-specific session), not polybot_news (E3 session)
    session_name = str(cfg.get("session_name", "polybot_weather"))
    disable_after = int(cfg.get("flash_disable_after_failures", 5))
    disable_min = int(cfg.get("flash_disable_minutes", 15))
    # F26 fix: recreate HTTP/Gamma/network sessions every SESSION_TTL_SEC to avoid
    # silent connection staleness from positive_vector-lived aiohttp connections.
    session_ttl = float(cfg.get("session_ttl_sec", 3600.0))

    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    telethon_tupper_bound = asyncio.create_tupper_bound(
        _telethon_run(queue, channels, session_name),
        name="E4_news_telethon",
    )

    logger.info(
        "E4 weather_news scanner online — %d channels min_conf=%.2f session=%s",
        len(channels), float(cfg.get("min_confidence", 0.60)), session_name,
    )

    # F18: polybot_weather.session must be created first via scripts/telegram_auth.py.
    # If the session does not exist, _telethon_run will log a clear warning and
    # sleep 3600s before retrying. No code change needed here — just doc.

    try:
        while True:
            # F26: outer loop recreates sessions on TTL expiry or crash
            session_started = time.monotonic()
            try:
                async with aiohttp.ClientSession() as http_session, \
                           GammaClient() as gamma, \
                           NetworkClient() as network:
                    while True:
                        # Renew sessions on TTL
                        if time.monotonic() - session_started > session_ttl:
                            logger.debug("E4 weather_news: renewing HTTP/Gamma/network sessions (TTL)")
                            break

                        try:
                            post = await asyncio.wait_for(queue.get(), timeout=30.0)
                        except asyncio.TimeoutError:
                            continue
                        except asyncio.CancelledError:
                            raise

                        try:
                            pid = post.get("id")
                            if pid is None or pid in _state.seen_ids:
                                continue
                            if _post_age_sec(post) > max_age:
                                _state.seen_ids.append(pid)
                                continue

                            body = (post.get("body") or post.get("title") or "").strip()
                            if not body:
                                _state.seen_ids.append(pid)
                                continue

                            if _flash_disabled():
                                continue

                            inf = await _classify(http_session, body, flash_to)
                            _state.seen_ids.append(pid)
                            if inf is None:
                                continue

                            await _emit_for_inference(inf, cfg, gamma, network, emit, post)

                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logger.exception("E4 weather_news iteration failed")
                            _record_flash_failure(disable_after, disable_min)
                            await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("E4 weather_news session layer crashed — restarting in 30s")
                await asyncio.sleep(30)
    finally:
        telethon_tupper_bound.cancel()
        try:
            await telethon_tupper_bound
        except (asyncio.CancelledError, Exception):
            pass
