#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Portfolio Guard (global breaker + daily risk brain)

Purpose
-------
Single source of truth for:
  - Daily equity anchor (session_start_equity)
  - Running PnL (realized + unrealized)
  - Daily loss limits (absolute + %)
  - Global breaker state (ON/OFF + reason)

This module wraps the guard_state helpers provided by core.db / app.core.db:
  - guard_load() -> Dict[str, Any]
  - guard_update_pnl(delta_realized_usd: Decimal, delta_unrealized_usd: Decimal = 0) -> None
  - guard_reset_day(equity_now_usd: Decimal) -> None

Bots that should call this:
  - Executor:
      • Before opening *any* new trade, call can_open_trade(...).
  - Risk / PnL trackers:
      • When fills occur, call record_pnl(...).
  - Higher level controllers:
      • May call trip_breaker(...) / clear_breaker(...).

Env / config knobs
------------------
PG_MAX_DAILY_LOSS_PCT     (float, default 3.0)   -> Max % loss from daily anchor
PG_MAX_DAILY_LOSS_USD     (float, default 0.0)   -> Absolute loss cap (0 = disabled)
PG_MAX_RISK_PER_TRADE_PCT (float, default 0.75)  -> Max % of *equity_now* risk per trade
PG_MAX_RISK_PER_TRADE_USD (float, default 0.0)   -> Absolute per-trade risk cap (0 = disabled)
PG_NOTIFY_BREAKER         (bool, default true)   -> Telegram notifications on trips
"""

from __future__ import annotations

import os
import datetime as _dt
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Tuple, Optional

# --------------------------------------------------------------------------
# DB guard state helpers (tolerant imports)
# --------------------------------------------------------------------------

try:
    # Preferred: top-level core package
    from core.db import guard_load, guard_update_pnl, guard_reset_day  # type: ignore
except Exception:
    try:
        # Fallback: app.core package
        from app.core.db import guard_load, guard_update_pnl, guard_reset_day  # type: ignore
    except Exception as e:  # pragma: no cover
        # If this fails, your DB wiring is broken. Let it raise.
        raise ImportError("Portfolio Guard cannot import DB guard helpers") from e

# --------------------------------------------------------------------------
# Telegram notifier (tolerant)
# --------------------------------------------------------------------------

try:
    from core.flashback_common import send_tg  # type: ignore
except Exception:
    try:
        from app.core.flashback_common import send_tg  # type: ignore
    except Exception:
        # Very last resort: no-op if import fails
        def send_tg(msg: str) -> None:  # type: ignore
            return


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

def _to_decimal(val: str, default: Decimal) -> Decimal:
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _env_decimal(name: str, default: str) -> Decimal:
    raw = os.getenv(name, default)
    return _to_decimal(raw, Decimal(default))


class GuardConfig:
    """Configuration for portfolio guard thresholds."""

    def __init__(self) -> None:
        # Max DAILY drawdown from daily anchor (percentage)
        self.max_daily_loss_pct: Decimal = _env_decimal("PG_MAX_DAILY_LOSS_PCT", "3.0")

        # Max DAILY absolute loss in USD (0 => disabled)
        self.max_daily_loss_usd: Decimal = _env_decimal("PG_MAX_DAILY_LOSS_USD", "0.0")

        # Per-trade risk cap (% of current equity)
        self.max_risk_per_trade_pct: Decimal = _env_decimal(
            "PG_MAX_RISK_PER_TRADE_PCT",
            "0.75",
        )

        # Per-trade absolute risk cap (0 => disabled)
        self.max_risk_per_trade_usd: Decimal = _env_decimal(
            "PG_MAX_RISK_PER_TRADE_USD",
            "0.0",
        )

        # Notify on breaker trips
        self.notify_breaker: bool = (
            os.getenv("PG_NOTIFY_BREAKER", "true").strip().lower() == "true"
        )


_CFG = GuardConfig()


# --------------------------------------------------------------------------
# State helpers
# --------------------------------------------------------------------------

def _normalize_state(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Normalize guard_state row into a stable dict with sane defaults.

    Expected DB shape (core/db.py):
      {
        "session_date": "YYYY-MM-DD",
        "session_start_equity": <Decimal/str>,
        "realized_pnl": <Decimal/str>,
        "unrealized_pnl": <Decimal/str>,
        "breaker_on": 0/1/bool,
        "breaker_reason": str or None,
        "last_updated_ms": int,
      }
    Anything missing is defaulted so callers don't explode.
    """
    today = _dt.date.today().isoformat()

    if not raw:
        return {
            "session_date": today,
            "session_start_equity": Decimal("0"),
            "realized_pnl": Decimal("0"),
            "unrealized_pnl": Decimal("0"),
            "breaker_on": False,
            "breaker_reason": None,
            "last_updated_ms": 0,
        }

    def d(key: str, default: str = "0") -> Decimal:
        try:
            return Decimal(str(raw.get(key, default)))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(default)

    return {
        "session_date": str(raw.get("session_date") or today),
        "session_start_equity": d("session_start_equity", "0"),
        "realized_pnl": d("realized_pnl", "0"),
        "unrealized_pnl": d("unrealized_pnl", "0"),
        "breaker_on": bool(raw.get("breaker_on", False)),
        "breaker_reason": raw.get("breaker_reason"),
        "last_updated_ms": int(raw.get("last_updated_ms", 0) or 0),
    }


def get_state() -> Dict[str, Any]:
    """
    Load guard_state from DB and normalize it.

    Cheap read, safe to call frequently.
    """
    raw = guard_load()
    return _normalize_state(raw)


def ensure_today(equity_now_usd: Decimal) -> Dict[str, Any]:
    """
    Ensure guard_state is anchored to *today*.

    If session_date != today, performs a guard_reset_day(equity_now_usd)
    and returns the fresh state. Otherwise, returns the existing state.
    """
    state = get_state()
    today = _dt.date.today().isoformat()

    if state["session_date"] != today:
        # New session / new day anchor
        guard_reset_day(equity_now_usd)
        state = get_state()

    # If we have a zero anchor but non-zero equity, we can lazily backfill.
    if state["session_start_equity"] <= 0 and equity_now_usd > 0:
        guard_reset_day(equity_now_usd)
        state = get_state()

    return state


def record_pnl(delta_realized_usd: Decimal,
               delta_unrealized_usd: Decimal = Decimal("0")) -> None:
    """
    Update PnL deltas in DB.

    Typically called by:
      - Trade journal / execution listener after fills.
      - A periodic mark-to-market process for unrealized.
    """
    guard_update_pnl(delta_realized_usd, delta_unrealized_usd)


# --------------------------------------------------------------------------
# Breaker controls
# --------------------------------------------------------------------------

def _notify_breaker(msg: str) -> None:
    if not _CFG.notify_breaker:
        return
    try:
        send_tg(f"🛑 Portfolio Breaker | {msg}")
    except Exception:
        # No one needs to die over a Telegram outage.
        pass


def _maybe_guard_set_breaker(on: bool, reason: str) -> None:
    """
    Try to persist breaker flag if DB helper exists. Safe fallback if not.
    """
    try:
        try:
            from core.db import guard_set_breaker  # type: ignore
        except Exception:
            from app.core.db import guard_set_breaker  # type: ignore
        guard_set_breaker(on, reason)
    except Exception:
        # If guard_set_breaker doesn't exist yet, skip silently.
        pass


def trip_breaker(reason: str,
                 meta: Optional[Dict[str, Any]] = None) -> None:
    """
    Trip the global breaker for today.

    Reason should be a short human-readable string, e.g.:
      "Daily loss limit hit: -3.2%"
      "Risk per trade exceeded"
    """
    state = get_state()
    if state["breaker_on"]:
        # Already tripped; no need to spam notifications.
        return

    msg = reason
    if meta:
        # Add a compact meta summary if present.
        details = ", ".join(f"{k}={v}" for k, v in meta.items())
        msg = f"{reason} ({details})"

    _notify_breaker(msg)
    _maybe_guard_set_breaker(True, reason)


def clear_breaker(reason: Optional[str] = None) -> None:
    """
    Clear the global breaker flag for today (manual override).

    Use this when:
      - You've reviewed the damage.
      - You consciously want to re-enable trading.
    """
    state = get_state()
    if not state["breaker_on"]:
        return

    why = reason or "Manual reset"
    _notify_breaker(f"Breaker cleared: {why}")
    _maybe_guard_set_breaker(False, why)


# --------------------------------------------------------------------------
# Risk checks
# --------------------------------------------------------------------------

def _compute_equity(state: Dict[str, Any],
                    equity_now_usd: Decimal) -> Tuple[Decimal, Decimal, Decimal]:
    """
    Compute:
      - anchor_equity   (session_start_equity)
      - current_equity  (equity_now_usd or anchor + pnl)
      - dd_usd          (drawdown in USD, negative when losing)
    """
    anchor = state["session_start_equity"]

    if equity_now_usd > 0:
        current = equity_now_usd
    else:
        # Fallback if we aren't passed real-time equity.
        current = anchor + state["realized_pnl"] + state["unrealized_pnl"]

    dd_usd = current - anchor
    return anchor, current, dd_usd


def _check_daily_limits(state: Dict[str, Any],
                        equity_now_usd: Decimal) -> Tuple[bool, Optional[str]]:
    """
    Check whether current drawdown exceeds configured daily limits.

    Returns (allowed, reason_if_blocked).
    """
    anchor, current, dd_usd = _compute_equity(state, equity_now_usd)

    if anchor <= 0:
        # No meaningful anchor yet; allow but don't enforce.
        return True, None

    # Drawdown is negative when losing
    if dd_usd >= 0:
        return True, None

    loss_usd = -dd_usd
    loss_pct = (loss_usd / anchor) * Decimal("100")

    # Percent cap
    if _CFG.max_daily_loss_pct > 0 and loss_pct >= _CFG.max_daily_loss_pct:
        reason = (
            f"Daily loss limit hit: {loss_pct:.2f}% "
            f"(max {_CFG.max_daily_loss_pct}%)"
        )
        return False, reason

    # Absolute cap
    if _CFG.max_daily_loss_usd > 0 and loss_usd >= _CFG.max_daily_loss_usd:
        reason = (
            f"Daily loss USD limit hit: -{loss_usd:.2f} "
            f"(max {_CFG.max_daily_loss_usd})"
        )
        return False, reason

    return True, None


def _check_per_trade_risk(risk_usd: Decimal,
                          equity_now_usd: Decimal) -> Tuple[bool, Optional[str]]:
    """
    Check whether proposed per-trade risk is within configured limits.
    """
    if risk_usd <= 0:
        return True, None

    # Percent-of-equity cap
    if _CFG.max_risk_per_trade_pct > 0 and equity_now_usd > 0:
        pct = (risk_usd / equity_now_usd) * Decimal("100")
        if pct > _CFG.max_risk_per_trade_pct:
            reason = (
                f"Per-trade risk {pct:.2f}% exceeds cap "
                f"({_CFG.max_risk_per_trade_pct}%)."
            )
            return False, reason

    # Absolute cap
    if _CFG.max_risk_per_trade_usd > 0 and risk_usd > _CFG.max_risk_per_trade_usd:
        reason = (
            f"Per-trade risk {risk_usd:.2f} exceeds absolute cap "
            f"({_CFG.max_risk_per_trade_usd})."
        )
        return False, reason

    return True, None


def can_open_trade(sub_uid: str,
                   strategy_name: str,
                   risk_usd: Decimal,
                   equity_now_usd: Decimal) -> Tuple[bool, Optional[str]]:
    """
    Main entry point for executor.

    Returns (allowed, reason_if_blocked).

    Execution flow:
      1) Ensure we have a session for today: ensure_today(equity_now_usd).
      2) If breaker_on: block any new trades.
      3) Check daily loss limits (anchor vs current).
      4) Check per-trade risk limits.
    """
    # 1) Ensure session state for today
    state = ensure_today(equity_now_usd)

    # 2) Global breaker switch
    if state["breaker_on"]:
        reason = state.get("breaker_reason") or "Global breaker ON"
        full_reason = f"Breaker ON for {sub_uid}/{strategy_name}: {reason}"
        return False, full_reason

    # 3) Daily limits
    ok_daily, reason_daily = _check_daily_limits(state, equity_now_usd)
    if not ok_daily:
        meta = {
            "sub_uid": sub_uid,
            "strategy": strategy_name,
        }
        trip_breaker(reason_daily or "Daily loss limit hit", meta=meta)
        full_reason = f"{reason_daily} (sub={sub_uid}, strat={strategy_name})"
        return False, full_reason

    # 4) Per-trade limits
    ok_risk, reason_risk = _check_per_trade_risk(risk_usd, equity_now_usd)
    if not ok_risk:
        # We don't necessarily trip the global breaker here, we just block this trade.
        full_reason = f"{reason_risk} (sub={sub_uid}, strat={strategy_name})"
        return False, full_reason

    return True, None
