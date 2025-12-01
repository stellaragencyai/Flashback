# app/ai/setup_memory_policy.py
# Flashback — Setup Memory Policy v1.0
#
# Purpose
# -------
# Central place to store and retrieve:
#   - Per-strategy risk multipliers (0.0–2.0+)
#   - Per-strategy minimum AI score thresholds (0.0–1.0)
#
# Used by executor_v2 via:
#   from app.ai.setup_memory_policy import get_risk_multiplier, get_min_ai_score
#
# Storage:
#   state/setup_policy.json
#
# Schema example:
# {
#   "schema_version": 1,
#   "defaults": {
#     "risk_multiplier": 1.0,
#     "min_ai_score": 0.0
#   },
#   "strategies": {
#     "Sub2_Breakout(524633243)": {
#       "risk_multiplier": 0.75,
#       "min_ai_score": 0.55
#     },
#     "Sub7_Canary(111222333)": {
#       "risk_multiplier": 0.25,
#       "min_ai_score": 0.30
#     }
#   }
# }
#
# v2 (later) can add:
#   - Auto-updating policies from performance
#   - Separate policies per regime
#   - Cooldowns after drawdowns

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from app.core.logger import get_logger
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


log = get_logger("setup_memory_policy")

# ROOT / state dir
try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POLICY_PATH: Path = STATE_DIR / "setup_policy.json"

# In-memory cache
_POLICY_CACHE: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _default_policy() -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "defaults": {
            "risk_multiplier": 1.0,
            "min_ai_score": 0.0,
        },
        "strategies": {},
    }


def _load_policy() -> Dict[str, Any]:
    global _POLICY_CACHE
    if _POLICY_CACHE is not None:
        return _POLICY_CACHE

    if not POLICY_PATH.exists():
        policy = _default_policy()
        _POLICY_CACHE = policy
        return policy

    try:
        raw = POLICY_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("policy root is not dict")
    except Exception as e:
        log.warning("Failed to load setup_policy.json, using defaults: %r", e)
        data = _default_policy()

    if "defaults" not in data or not isinstance(data["defaults"], dict):
        data["defaults"] = _default_policy()["defaults"]
    if "strategies" not in data or not isinstance(data["strategies"], dict):
        data["strategies"] = {}

    _POLICY_CACHE = data
    return data


def _save_policy(policy: Dict[str, Any]) -> None:
    global _POLICY_CACHE
    _POLICY_CACHE = policy
    try:
        POLICY_PATH.write_text(json.dumps(policy, indent=2), encoding="utf-8")
    except Exception as e:
        log.error("Failed to save setup_policy.json: %r", e)


def _get_strategy_block(strategy_id: str) -> Dict[str, Any]:
    policy = _load_policy()
    strategies = policy.get("strategies", {})
    block = strategies.get(strategy_id)
    if not isinstance(block, dict):
        block = {}
    return block


# ---------------------------------------------------------------------------
# Public getters used by executor_v2
# ---------------------------------------------------------------------------

def get_risk_multiplier(strategy_id: str) -> float:
    """
    Return per-strategy risk multiplier.

    - 1.0 means "use raw risk_pct from strategies.yaml"
    - <1.0 shrinks risk (e.g. 0.5 = half risk)
    - >1.0 increases risk (if you ever decide this is sane)

    If strategy not found, falls back to default (1.0).
    """
    policy = _load_policy()
    defaults = policy.get("defaults", {})
    default_mult = defaults.get("risk_multiplier", 1.0)

    block = _get_strategy_block(strategy_id)
    val = block.get("risk_multiplier", default_mult)

    try:
        d = Decimal(str(val))
        if d <= 0:
            return float(default_mult)
        return float(d)
    except Exception:
        return float(default_mult)


def get_min_ai_score(strategy_id: str) -> float:
    """
    Return minimum AI score required for this strategy.

    - 0.0 means "no minimum, treat all allowed by model"
    - 0.5 means "at least 50% model probability"
    - 0.7+ means "only take high conviction trades"

    If strategy not found, falls back to default (0.0).
    """
    policy = _load_policy()
    defaults = policy.get("defaults", {})
    default_min = defaults.get("min_ai_score", 0.0)

    block = _get_strategy_block(strategy_id)
    val = block.get("min_ai_score", default_min)

    try:
        d = Decimal(str(val))
        if d < 0:
            return 0.0
        if d > 1:
            return 1.0
        return float(d)
    except Exception:
        return float(default_min)


# ---------------------------------------------------------------------------
# Optional setters (for future Telegram/UI control)
# ---------------------------------------------------------------------------

def set_risk_multiplier(strategy_id: str, multiplier: float) -> None:
    """
    Update the risk multiplier for a given strategy and persist it.
    """
    policy = _load_policy()
    strategies = policy.setdefault("strategies", {})

    block = strategies.get(strategy_id) or {}
    block["risk_multiplier"] = float(multiplier)
    strategies[strategy_id] = block
    policy["strategies"] = strategies
    _save_policy(policy)
    log.info("Set risk_multiplier=%.4f for strategy_id=%s", multiplier, strategy_id)


def set_min_ai_score(strategy_id: str, min_score: float) -> None:
    """
    Update the min AI score threshold for a given strategy and persist it.
    """
    # clamp to [0,1]
    try:
        d = Decimal(str(min_score))
        if d < 0:
            d = Decimal("0")
        if d > 1:
            d = Decimal("1")
        val = float(d)
    except Exception:
        val = 0.0

    policy = _load_policy()
    strategies = policy.setdefault("strategies", {})

    block = strategies.get(strategy_id) or {}
    block["min_ai_score"] = val
    strategies[strategy_id] = block
    policy["strategies"] = strategies
    _save_policy(policy)
    log.info("Set min_ai_score=%.4f for strategy_id=%s", val, strategy_id)


def set_default_risk_multiplier(multiplier: float) -> None:
    """
    Set global default risk multiplier (applies when strategy has no override).
    """
    policy = _load_policy()
    defaults = policy.setdefault("defaults", {})
    defaults["risk_multiplier"] = float(multiplier)
    policy["defaults"] = defaults
    _save_policy(policy)
    log.info("Set DEFAULT risk_multiplier=%.4f", multiplier)


def set_default_min_ai_score(min_score: float) -> None:
    """
    Set global default min AI score (applies when strategy has no override).
    """
    try:
        d = Decimal(str(min_score))
        if d < 0:
            d = Decimal("0")
        if d > 1:
            d = Decimal("1")
        val = float(d)
    except Exception:
        val = 0.0

    policy = _load_policy()
    defaults = policy.setdefault("defaults", {})
    defaults["min_ai_score"] = val
    policy["defaults"] = defaults
    _save_policy(policy)
    log.info("Set DEFAULT min_ai_score=%.4f", val)


if __name__ == "__main__":
    # Simple CLI helper for quick inspection
    import sys

    if len(sys.argv) == 1:
        p = _load_policy()
        print(json.dumps(p, indent=2))
    elif len(sys.argv) == 3 and sys.argv[1] == "get":
        sid = sys.argv[2]
        print("strategy:", sid)
        print("risk_multiplier:", get_risk_multiplier(sid))
        print("min_ai_score:", get_min_ai_score(sid))
    else:
        print("Usage:")
        print("  python -m app.ai.setup_memory_policy")
        print("  python -m app.ai.setup_memory_policy get <strategy_id>")
