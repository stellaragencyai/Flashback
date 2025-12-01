# app/core/portfolio_guard.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Portfolio Guard

Purpose
-------
Central risk gate in front of the executor.

Responsibilities:
- Enforce a global breaker switch (stop all new trades).
- Enforce simple per-trade risk caps, based on:
    • absolute USD risk (MAX_TRADE_RISK_USD)
    • % of current equity (MAX_TRADE_RISK_PCT)
- Provide a backwards-compatible interface:

New style (preferred):
    can_open_trade(
        sub_uid="524633243",
        strategy_name="Sub2_BO",
        risk_usd=Decimal("5.0"),
        equity_now_usd=Decimal("100.0"),
    ) -> (allowed: bool, reason: str)

Legacy style (for older callers):
    can_open_trade("BTCUSDT", 5.0) -> (allowed: bool, reason: str)

Notes
-----
This v1 guard is intentionally simple. It does NOT:
- Track open positions.
- Track daily loss.
- Track symbol concentration.

Those can be layered on later using the event_bus + journal.
"""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Tuple

from app.core.logger import get_logger

log = get_logger("portfolio_guard")


# ---------- ENV HELPERS ----------

def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_decimal(name: str, default: str = "0") -> Decimal:
    raw = os.getenv(name, default)
    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError):
        log.warning("Invalid decimal in env %s=%r; falling back to %s", name, raw, default)
        return Decimal(default)


def _to_decimal(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError):
        return Decimal("0")


# ---------- CONFIG / STATE ----------

# Global breaker: if true, no new trades are allowed.
# Seeded from env, but can now be toggled at runtime via set_global_breaker().
_GLOBAL_BREAKER: bool = _env_bool("GLOBAL_BREAKER", False)
_GLOBAL_BREAKER_REASON: str = "env_default" if _GLOBAL_BREAKER else ""

# Per-trade risk caps (0 => disabled)
# Example you *could* set in .env later:
#   MAX_TRADE_RISK_PCT=0.05        # 5% of equity max per trade
#   MAX_TRADE_RISK_USD=10          # or $10 absolute cap
MAX_TRADE_RISK_PCT: Decimal = _env_decimal("MAX_TRADE_RISK_PCT", "0")   # fraction, e.g. 0.05
MAX_TRADE_RISK_USD: Decimal = _env_decimal("MAX_TRADE_RISK_USD", "0")   # USD amount


# ---------- BREAKER UTILITIES (new) ----------

def is_breaker_active() -> bool:
    """
    Return True if the global breaker is currently active.
    """
    return _GLOBAL_BREAKER


def set_global_breaker(active: bool, reason: str = "") -> None:
    """
    Toggle the global breaker at runtime.

    This lets higher-level controllers (e.g. daily DD guard, manual
    Telegram command, etc.) flip the breaker without restarting
    the process.

    reason is only used for logging/inspection; can be empty.
    """
    global _GLOBAL_BREAKER, _GLOBAL_BREAKER_REASON
    _GLOBAL_BREAKER = bool(active)
    _GLOBAL_BREAKER_REASON = str(reason or "")
    log.warning("Global breaker set to %s (reason=%s)", _GLOBAL_BREAKER, _GLOBAL_BREAKER_REASON)


def get_guard_limits() -> Dict[str, Any]:
    """
    Introspection helper: return current guard limits and breaker state.

    Useful for:
      - Debug endpoints
      - Status panels
      - Bots that want to log config on startup
    """
    return {
        "global_breaker": _GLOBAL_BREAKER,
        "global_breaker_reason": _GLOBAL_BREAKER_REASON,
        "max_trade_risk_usd": str(MAX_TRADE_RISK_USD),
        "max_trade_risk_pct": str(MAX_TRADE_RISK_PCT),
    }


# ---------- CORE IMPLEMENTATION (new-style) ----------

def _can_open_trade_new(
    sub_uid: str,
    strategy_name: str,
    risk_usd: Decimal,
    equity_now_usd: Decimal,
) -> Tuple[bool, str]:
    """
    New-style guard.

    All args must already be Decimals (or convertible).
    Returns (allowed, reason).
    """
    # 1) Global breaker
    if is_breaker_active():
        log.warning(
            "Guard blocked trade [sub_uid=%s, strat=%s]: GLOBAL_BREAKER active (reason=%s).",
            sub_uid,
            strategy_name,
            _GLOBAL_BREAKER_REASON,
        )
        return False, "global_breaker_active"

    # 2) Sanity checks
    if risk_usd <= 0:
        return False, "non_positive_risk"
    if equity_now_usd <= 0:
        return False, "no_equity"

    # 3) Absolute USD cap
    if MAX_TRADE_RISK_USD > 0 and risk_usd > MAX_TRADE_RISK_USD:
        log.info(
            "Guard blocked trade [sub_uid=%s, strat=%s]: risk_usd=%s > MAX_TRADE_RISK_USD=%s",
            sub_uid,
            strategy_name,
            risk_usd,
            MAX_TRADE_RISK_USD,
        )
        return False, "risk_usd_above_cap"

    # 4) % of equity cap
    if MAX_TRADE_RISK_PCT > 0:
        max_allowed = equity_now_usd * MAX_TRADE_RISK_PCT
        if risk_usd > max_allowed:
            log.info(
                "Guard blocked trade [sub_uid=%s, strat=%s]: risk_usd=%s > max_pct_cap=%s (pct=%s)",
                sub_uid,
                strategy_name,
                risk_usd,
                max_allowed,
                MAX_TRADE_RISK_PCT,
            )
            return False, "risk_pct_above_cap"

    # 5) If we reach here, it's allowed
    return True, "ok"


# ---------- PUBLIC ENTRYPOINT (supports new + legacy) ----------

def can_open_trade(*args, **kwargs) -> Tuple[bool, str]:
    """
    Unified portfolio guard entrypoint.

    New-style usage (preferred):
        can_open_trade(
            sub_uid="524633243",
            strategy_name="Sub2_BO",
            risk_usd=Decimal("5.0"),
            equity_now_usd=Decimal("100.0"),
        )

    Legacy usage:
        can_open_trade("BTCUSDT", 5.0)

    Returns:
        (allowed: bool, reason: str)
    """

    # --- New-style path: keyword arguments present ---
    if kwargs:
        sub_uid = str(kwargs.get("sub_uid") or "")
        strategy_name = str(kwargs.get("strategy_name") or "")
        risk_usd = _to_decimal(kwargs.get("risk_usd", 0))
        equity_now_usd = _to_decimal(kwargs.get("equity_now_usd", 0))

        allowed, reason = _can_open_trade_new(
            sub_uid=sub_uid,
            strategy_name=strategy_name,
            risk_usd=risk_usd,
            equity_now_usd=equity_now_usd,
        )
        return allowed, reason

    # --- Legacy path: positional (symbol, risk_usd) ---
    if len(args) >= 2:
        symbol = str(args[0])
        risk_usd = _to_decimal(args[1])

        if is_breaker_active():
            log.warning(
                "Legacy guard blocked trade [symbol=%s]: GLOBAL_BREAKER active (reason=%s).",
                symbol,
                _GLOBAL_BREAKER_REASON,
            )
            return False, "global_breaker_active"

        if risk_usd <= 0:
            return False, "non_positive_risk"

        # No equity info in legacy mode, so we can only enforce absolute cap.
        if MAX_TRADE_RISK_USD > 0 and risk_usd > MAX_TRADE_RISK_USD:
            log.info(
                "Legacy guard blocked trade [symbol=%s]: risk_usd=%s > MAX_TRADE_RISK_USD=%s",
                symbol,
                risk_usd,
                MAX_TRADE_RISK_USD,
            )
            return False, "risk_usd_above_cap"

        # Otherwise allow.
        return True, "legacy_ok"

    # --- Completely invalid usage ---
    log.warning("can_open_trade called with invalid arguments: args=%r kwargs=%r", args, kwargs)
    return False, "invalid_arguments"
