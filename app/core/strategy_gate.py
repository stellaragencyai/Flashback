#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Strategy Gate

Purpose
-------
Single source of truth for answering questions like:

  • "Given this symbol + timeframe, which strategies care about it?"
  • "For sub_uid X, what is its risk, mode, and symbols/TFs?"
  • "Is this strategy currently allowed to auto-trade or only learn/paper?"

This sits on top of:
    - config/strategies.yaml
    - app.core.strategies

Typical usage (executor, AI gate, dashboards):
    from app.core.strategy_gate import (
        get_strategies_for_signal,
        get_strategy_for_sub,
        is_strategy_live,
        is_strategy_enabled,
        strategy_risk_pct,
    )

    sig = {"symbol": "BTCUSDT", "timeframe": "5m", "side": "LONG"}
    matches = get_strategies_for_signal(sig["symbol"], sig["timeframe"])
    for strat in matches:
        if not is_strategy_enabled(strat):
            continue
        if is_strategy_live(strat):
            # LIVE_CANARY / LIVE_FULL entry logic
            ...
        else:
            # LEARN_DRY / OFF -> logging / AI only
            ...

Modes (Option A)
----------------
We keep the richer 4-mode automation scheme used by executor_v2:

    OFF         : ignore strategy
    LEARN_DRY   : learn-only / paper mode (log, AI, no live orders)
    LIVE_CANARY : small-size live trades (canary / experimental)
    LIVE_FULL   : normal live trading

Timeframes
----------
strategies.yaml uses raw minute intervals as strings ("1", "5", "15", "60", "240"...).
Signal engine may emit "5", "5m", "1h", etc. We normalize both sides to raw minute strings.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from app.core import strategies as stratreg


# --------- Helpers for timeframe normalization ---------


def _normalize_tf(tf: str) -> str:
    """
    Normalize timeframe strings.

    Accepted inputs:
        "5"   -> "5"
        "5m"  -> "5"
        "15"  -> "15"
        "1h"  -> "60"
        "60"  -> "60"
        "4h"  -> "240"
        "240" -> "240"

    We keep strategies.yaml in raw-minute form ("1", "5", "15", "60", "240", ...).
    """
    s = str(tf).strip().lower()

    # pure integer string -> minutes already
    if s.isdigit():
        return s

    # match like: 5m, 15m, 1h, 4h, 1d, etc.
    m = re.match(r"^(\d+)([mhd])$", s)
    if not m:
        # unknown pattern, just return as-is to avoid silent bugs
        return s

    val = int(m.group(1))
    unit = m.group(2)

    if unit == "m":
        return str(val)
    if unit == "h":
        return str(val * 60)
    if unit == "d":
        return str(val * 60 * 24)

    return s


# --------- Core accessors ---------


def _normalized_strategies() -> List[Dict[str, Any]]:
    """
    Load all strategies and attach some normalized fields:

        - "sub_uid_str"          : canonical string version of sub_uid
        - "automation_mode_norm" : OFF | LEARN_DRY | LIVE_CANARY | LIVE_FULL
        - "symbols_norm"         : [uppercased symbols]
        - "timeframes_norm"      : [normalized raw mins, as strings]
    """
    out: List[Dict[str, Any]] = []

    for s in stratreg.all_sub_strategies():
        sub_uid = str(s.get("sub_uid"))

        # normalize automation mode into the 4-mode scheme used by executor_v2
        mode_raw = str(s.get("automation_mode", "OFF")).strip().upper()
        if mode_raw not in ("OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"):
            # Fail-closed: any weird / missing value becomes OFF
            mode_raw = "OFF"

        symbols_raw = s.get("symbols") or []
        tfs_raw = s.get("timeframes") or []

        symbols_norm = [str(sym).upper().strip() for sym in symbols_raw if str(sym).strip()]
        tfs_norm = [_normalize_tf(tf) for tf in tfs_raw if str(tf).strip()]

        wrapped = dict(s)
        wrapped["sub_uid_str"] = sub_uid
        wrapped["automation_mode_norm"] = mode_raw
        wrapped["symbols_norm"] = symbols_norm
        wrapped["timeframes_norm"] = tfs_norm
        out.append(wrapped)

    return out


def all_strategies() -> List[Dict[str, Any]]:
    """
    Public: return all normalized strategies.
    """
    return _normalized_strategies()


def get_strategy_for_sub(sub_uid: str) -> Optional[Dict[str, Any]]:
    """
    Get the strategy dict for a given sub_uid (string or int).
    Returns normalized dict, or None if not found.
    """
    sub_uid_str = str(sub_uid)
    for s in _normalized_strategies():
        if s.get("sub_uid_str") == sub_uid_str:
            return s
    return None


def get_strategies_for_signal(symbol: str, timeframe: str) -> List[Dict[str, Any]]:
    """
    Given a signal (symbol + timeframe), return all strategies that
    should consider acting on it.

    Strategy matches if:
      - symbol is in its symbols list
      - normalized timeframe matches one of its timeframes

    The returned strategies are normalized and include:
        sub_uid_str, automation_mode_norm, symbols_norm, timeframes_norm
    """
    sym_u = str(symbol).upper().strip()
    tf_norm = _normalize_tf(timeframe)

    matches: List[Dict[str, Any]] = []
    for s in _normalized_strategies():
        if sym_u not in s.get("symbols_norm", []):
            continue
        if tf_norm not in s.get("timeframes_norm", []):
            continue
        matches.append(s)
    return matches


# --------- Automation mode helpers (4-mode aware) ---------


def strategy_mode(strategy: Dict[str, Any]) -> str:
    """
    Return the normalized automation mode for a strategy:
        OFF | LEARN_DRY | LIVE_CANARY | LIVE_FULL
    """
    mode = strategy.get("automation_mode_norm") or str(strategy.get("automation_mode", "")).upper().strip()
    if mode not in ("OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"):
        mode = "OFF"
    return mode


def is_strategy_enabled(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy is 'enabled' in strategies.yaml.
    (This is separate from automation_mode; you can disable a strategy entirely.)
    """
    return bool(strategy.get("enabled", False))


def is_strategy_off(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy's automation_mode is OFF.
    """
    return strategy_mode(strategy) == "OFF"


def is_strategy_learn_dry(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy's automation_mode is LEARN_DRY
    (learn-only / paper-style: AI + logging, no live orders).
    """
    return strategy_mode(strategy) == "LEARN_DRY"


def is_strategy_live_canary(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy's automation_mode is LIVE_CANARY.
    """
    return strategy_mode(strategy) == "LIVE_CANARY"


def is_strategy_live_full(strategy: Dict[str, Any]) -> bool:
    """
    Returns True if the strategy's automation_mode is LIVE_FULL.
    """
    return strategy_mode(strategy) == "LIVE_FULL"


def is_strategy_live(strategy: Dict[str, Any]) -> bool:
    """
    Convenience: returns True if the strategy is in ANY live mode:
        LIVE_CANARY or LIVE_FULL
    """
    m = strategy_mode(strategy)
    return m in ("LIVE_CANARY", "LIVE_FULL")


def is_strategy_paper(strategy: Dict[str, Any]) -> bool:
    """
    Backwards-compatible helper: treat LEARN_DRY as 'paper' mode.
    """
    return strategy_mode(strategy) == "LEARN_DRY"


# --------- Risk & concurrency helpers ---------


def strategy_risk_pct(strategy: Dict[str, Any]) -> float:
    """
    Convenience: get risk_per_trade_pct as float.
    If missing, defaults to 0.0.
    """
    try:
        return float(strategy.get("risk_per_trade_pct", 0.0))
    except Exception:
        return 0.0


def strategy_max_concurrent(strategy: Dict[str, Any]) -> int:
    """
    Convenience: get max_concurrent_positions as int.
    If missing, defaults to 1.
    """
    try:
        return int(strategy.get("max_concurrent_positions", 1))
    except Exception:
        return 1


def strategy_label(strategy: Dict[str, Any]) -> str:
    """
    Returns a nice label for logs/Telegram:
        "<name> (sub <uid>)"
    """
    sub_uid = strategy.get("sub_uid_str") or strategy.get("sub_uid")
    name = strategy.get("name") or stratreg.get_sub_label(str(sub_uid))
    return f"{name} (sub {sub_uid})"


def should_strategy_handle(symbol: str, timeframe: str) -> Dict[str, Dict[str, Any]]:
    """
    Adapter for callers that want a dict keyed by strategy name.

    Given symbol + timeframe, return:
        {
          "<strategy_name>": <strategy_dict>,
          ...
        }

    Strategy name is taken from:
      - strategy["name"] if present
      - otherwise a label from strategy_label(...)
    """
    matches = get_strategies_for_signal(symbol, timeframe)
    out: Dict[str, Dict[str, Any]] = {}
    for s in matches:
        name = s.get("name") or strategy_label(s)
        out[name] = s
    return out
