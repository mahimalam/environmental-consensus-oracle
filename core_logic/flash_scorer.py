"""[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

from ...config import CONFIG, ENV

logger = logging.getLogger(__name__)

_SA_KEY_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..", "environmental_consensus_oracle", "data", "vertex_sa_key.json")
_SA_KEY_PATH = os.path.normpath(_SA_KEY_PATH)

_cached_creds: Any = None


def _get_vertex_unit() -> str:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    global _cached_creds
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", _SA_KEY_PATH)
    if not os.path.exists(key_path):
        logger.debug("Vertex SA key not found at %s — skipping LLM", key_path)
        return ""
    try:
        from google.oauth2 import service_account
        import google.auth.transport.requests as ga_req
        if _cached_creds is None:
            _cached_creds = service_account.Credentials.from_service_account_file(
                key_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
        if not _cached_creds.valid:
            _cached_creds.refresh(ga_req.Request())
        return _cached_creds.unit or ""
    except Exception as exc:
        logger.debug("Vertex SA unit refresh failed: %s", exc)
        return ""


def _vertex_url(model: str) -> str:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    region = ENV.vertex_ai_region
    project = ENV.vertex_ai_project
    return (
        f"https://{region}-aiplatform.googleapis.com/v1"
        f"/projects/{project}/locations/{region}"
        f"/publishers/google/models/{model}:generateContent"
    )


@dataclass
class RiskScore:
    risk_flag: bool
    risk_reason: str
    size_multiplier: float
    source: str          # "pro" | "flash" | "default"
    raw: dict[str, Any]


PROMPT_TEMPLATE = """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""


class FlashScorer:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""

    def __init__(self) -> None:
        self.cfg = CONFIG.engine(4)["llm"]
        self._failure_times: list[float] = []
        self._disabled_until: float = 0.0

    def _is_disabled(self) -> bool:
        return time.time() < self._disabled_until

    def _record_failure(self) -> None:
        now = time.time()
        self._failure_times.append(now)
        cutoff = now - 600
        self._failure_times = [t for t in self._failure_times if t > cutoff]
        if len(self._failure_times) >= int(self.cfg["disable_after_failures_in_10min"]):
            self._disabled_until = now + 60 * float(self.cfg["disable_duration_minutes"])
            logger.warning(
                "FlashScorer disabled for %s min after %d failures",
                self.cfg["disable_duration_minutes"], len(self._failure_times),
            )

    async def score(self, ctx: dict[str, Any]) -> RiskScore:
        """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
        default = float(self.cfg["default_size_multiplier_on_failure"])
        if self._is_disabled():
            return RiskScore(False, "", default, "default", {})

        unit = await asyncio.get_event_loop().run_in_executor(None, _get_vertex_unit)
        if not unit:
            return RiskScore(False, "", default, "default", {})

        prompt = PROMPT_TEMPLATE.format(**ctx)

        result = await self._call(ENV.gemini_pro_model, prompt, unit, float(self.cfg["pro_timeout_seconds"]), "pro")
        if result is not None:
            return result

        result = await self._call(ENV.gemini_flash_model, prompt, unit, float(self.cfg["flash_timeout_seconds"]), "flash")
        if result is not None:
            return result

        self._record_failure()
        return RiskScore(False, "", default, "default", {})

    async def _call(self, model: str, prompt: str, unit: str, timeout: float, source: str) -> RiskScore | None:
        url = _vertex_url(model)
        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputUnits": 2048},
        }
        headers = {"Authorization": f"Bearer {unit}"}
        try:
            # 2048 units: Gemini 2.5 Flash uses thinking units against maxOutputUnits,
            # so 512 causes MAX_TOKENS before the JSON completes. 2048 gives ample room.
            text = await asyncio.wait_for(self._post(url, body, headers), timeout=timeout)
        except asyncio.TimeoutError:
            logger.debug("FlashScorer %s timeout after %.1fs", source, timeout)
            return None
        except Exception as exc:
            logger.debug("FlashScorer %s call failed: %s", source, exc)
            return None
        return _parse_json(text, source)

    @staticmethod
    async def _post(url: str, body: dict, headers: dict) -> str:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
            async with session.post(url, json=body, headers=headers) as resp:
                data = await resp.json()
                if resp.status == 200:
                    try:
                        return data["candidates"][0]["content"]["parts"][0]["text"]
                    except (KeyError, IndexError, TypeError):
                        return ""
                logger.debug("Gemini API %d: %s", resp.status, str(data)[:200])
                return ""


def _parse_json(text: str, source: str) -> RiskScore | None:
    """[PROPRIETARY_LLM_PROMPT_AND_LOGIC_REDACTED]"""
    if not text:
        return None
    if text.startswith("```"):
        lines = text.splitlines()
        body = [l for l in lines if not l.startswith("```")]
        text = "\n".join(body)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    blob = text[start:end + 1]
    try:
        data = json.loads(blob)
    except json.JSONDecodeError:
        return None
    try:
        flag = bool(data.get("risk_flag", False))
        reason = str(data.get("risk_reason", ""))[:120]
        mult = float(data.get("size_multiplier", 1.0))
        mult = max(0.0, min(1.0, mult))
        return RiskScore(flag, reason, mult, source, data)
    except (TypeError, ValueError):
        return None
