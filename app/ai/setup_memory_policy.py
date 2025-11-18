#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Setup Memory Policy (v1)

Purpose
-------
Read:
  - state/setup_memory_summary.json   (built by setup_memory_stats.py)

Expose:
  - get_risk_multiplier(strategy_id: str) -> float
  - get_min_ai_score(strategy_id: str) -> float

So the live executor can:
  - Scale risk_per_trade_pct per strategy (canaries, strong vs weak).
  - Adjust how picky the AI gate should be (min score) per strategy.

This is deliberately simple & deterministic for v1.
No fancy ML here, just "if it wins more, trust it a bit more".
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional

import orjson

# tolerant settings import
try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_PATH = STATE_DIR / "setup_memory_summary.json"


# ------------- config knobs ------------- #

MIN_TRADES_FOR_ADJUST = 30   # below this: no dynamic scaling
MAX_RISK_MULT = 2.0          # hard cap on how aggressive we allow
MIN_RISK_MULT = 0.25         # hard floor on how low we cut risk


# ------------- internal cache ------------- #

_policy_cache: Dict[str, Any] = {
    "loaded": False,
    "strategies": {},   # sid -> stats dict
}


def _load_summary() -> None:
    """
    Lazy-load summary JSON once per process.
    """
    if _policy_cache["loaded"]:
        return

    if not SUMMARY_PATH.exists():
        _policy_cache["strategies"] = {}
        _policy_cache["loaded"] = True
        return

    try:
        data = orjson.loads(SUMMARY_PATH.read_bytes())
    except Exception:
        _policy_cache["strategies"] = {}
        _policy_cache["loaded"] = True
        return

    by_strategy = data.get("by_strategy", {}) or {}
    _policy_cache["strategies"] = by_strategy
    _policy_cache["loaded"] = True


def _get_stats(strategy_id: str) -> Optional[Dict[str, Any]]:
    _load_summary()
    return _policy_cache["strategies"].get(strategy_id)


# ------------- risk multiplier policy ------------- #

def get_risk_multiplier(strategy_id: str) -> float:
    """
    Return a multiplicative factor to apply on top of the static
    risk_per_trade_pct from strategies.yaml.

    High-level rules:
      - Not enough data (trades < MIN_TRADES_FOR_ADJUST) -> 1.0
      - Strong strategy:
          winrate >= 60% AND avg_rr >= 1.5 -> 1.5x
          winrate >= 55% AND avg_rr >= 1.2 -> 1.25x
      - Weak strategy:
          winrate <= 45% OR  avg_rr <= 0.7 -> 0.5x
      - Otherwise: 1.0

    Final value is clamped to [MIN_RISK_MULT, MAX_RISK_MULT].
    """
    stats = _get_stats(strategy_id)
    if not stats:
        return 1.0

    trades = stats.get("trades", 0) or 0
    if trades < MIN_TRADES_FOR_ADJUST:
        return 1.0

    winrate = stats.get("winrate")
    avg_rr = stats.get("avg_rr")

    try:
        winrate_f = float(winrate) if winrate is not None else None
    except Exception:
        winrate_f = None

    try:
        avg_rr_f = float(avg_rr) if avg_rr is not None else None
    except Exception:
        avg_rr_f = None

    # no useful stats -> neutral
    if winrate_f is None or avg_rr_f is None:
        return 1.0

    # Strong performers
    if winrate_f >= 0.60 and avg_rr_f >= 1.5:
        mult = 1.5
    elif winrate_f >= 0.55 and avg_rr_f >= 1.2:
        mult = 1.25

    # Weak performers
    elif winrate_f <= 0.45 or avg_rr_f <= 0.7:
        mult = 0.5

    # Middle of the pack
    else:
        mult = 1.0

    # Clamp to protective bounds
    if mult > MAX_RISK_MULT:
        mult = MAX_RISK_MULT
    if mult < MIN_RISK_MULT:
        mult = MIN_RISK_MULT
    return float(mult)


# ------------- AI score threshold policy ------------- #

def get_min_ai_score(strategy_id: str) -> float:
    """
    Return a suggested minimum AI score threshold for a strategy.

    We don't enforce this anywhere yet; executor_v2 can choose to:

      - If ai_score < get_min_ai_score(strat_id): reject trade.

    Rules (simple v1):
      - Not enough data: return 0.0 (let the classifier decide).
      - Very strong stats (winrate >= 60%, avg_rr >= 1.5):
          -> min_score = 0.45 (we allow more trades from a proven winner)
      - Mediocre stats:
          -> min_score = 0.6
      - Weak stats:
          -> min_score = 0.7  (only take very confident setups)
    """
    stats = _get_stats(strategy_id)
    if not stats:
        return 0.0

    trades = stats.get("trades", 0) or 0
    if trades < MIN_TRADES_FOR_ADJUST:
        return 0.0

    winrate = stats.get("winrate")
    avg_rr = stats.get("avg_rr")

    try:
        winrate_f = float(winrate) if winrate is not None else None
    except Exception:
        winrate_f = None

    try:
        avg_rr_f = float(avg_rr) if avg_rr is not None else None
    except Exception:
        avg_rr_f = None

    if winrate_f is None or avg_rr_f is None:
        return 0.0

    # Strong
    if winrate_f >= 0.60 and avg_rr_f >= 1.5:
        return 0.45

    # Weak
    if winrate_f <= 0.45 or avg_rr_f <= 0.7:
        return 0.70

    # In between
    return 0.60
