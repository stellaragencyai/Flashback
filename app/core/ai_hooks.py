#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 / Flashback — AI Hooks

Thin convenience layer on top of:
- app.core.ai_schema
- app.core.ai_store (optional; falls back to stub if missing)

Purpose:
- Give bots a simple, stable API to log AI-relevant events:
    • log_signal_from_engine(...)
    • log_order_basic(...)
    • log_trade_summary_basic(...)

If ai_store is NOT available yet, this module will:
    - Not crash.
    - Print lightweight [AI_STORE_STUB] messages instead of persisting data.
"""

from __future__ import annotations

import time
import uuid
from typing import Optional, List, Dict, Any

from app.core.ai_schema import (
    SignalLog,
    OrderLog,
    TradeSummaryLog,
)

# ---------- ai_store (real or stub) ----------

_HAS_AI_STORE = False

try:
    # Try to use the real store implementation if you have it wired.
    from app.core import ai_store as _ai_store  # type: ignore
    _HAS_AI_STORE = True
except Exception:
    # Fallback: stubbed store that just prints to stdout so nothing crashes.
    class _StubAIStore:
        def log_signal(self, payload: SignalLog) -> None:
            sid = payload.get("signal_id")
            sym = payload.get("symbol")
            tf = payload.get("timeframe")
            side = payload.get("side")
            print(f"[AI_STORE_STUB] log_signal: {sid} {sym} {tf} {side}")

        def log_order(self, payload: OrderLog) -> None:
            oid = payload.get("order_id")
            sym = payload.get("symbol")
            print(f"[AI_STORE_STUB] log_order: {oid} {sym}")

        def log_trade_summary(self, payload: TradeSummaryLog) -> None:
            tid = payload.get("trade_id")
            sym = payload.get("symbol")
            outcome = payload.get("outcome")
            print(f"[AI_STORE_STUB] log_trade_summary: {tid} {sym} outcome={outcome}")

    _ai_store = _StubAIStore()  # type: ignore


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------- SIGNALS ----------

def log_signal_from_engine(
    *,
    symbol: str,
    timeframe: str,
    side: str,
    source: str = "signal_engine",
    confidence: Optional[float] = None,
    stop_hint: Optional[float] = None,
    owner: Optional[str] = None,
    sub_uid: Optional[str] = None,
    strategy_role: Optional[str] = None,
    regime_tags: Optional[List[str]] = None,
    extra: Optional[Dict[str, Any]] = None,
    signal_id: Optional[str] = None,
    ts_ms: Optional[int] = None,
) -> str:
    """
    Convenience wrapper for logging a signal.

    Returns:
        signal_id (str) that was used/stored.
    """
    if signal_id is None:
        # Slightly human-readable ID: <uuid4>-<symbol>-<tf>
        signal_id = f"{uuid.uuid4().hex}_{symbol}_{timeframe}"

    if ts_ms is None:
        ts_ms = _now_ms()

    payload: SignalLog = {
        "signal_id": signal_id,
        "ts_ms": ts_ms,
        "symbol": symbol,
        "timeframe": timeframe,
        "side": side,
        "source": source,
        "confidence": confidence,
        "stop_hint_price": stop_hint,
        "owner": owner,
        "sub_uid": sub_uid,
        "strategy_role": strategy_role,
        "regime_tags": regime_tags or [],
        "extra": extra or {},
    }

    _ai_store.log_signal(payload)
    return signal_id


# ---------- ORDERS ----------

def log_order_basic(
    *,
    order_id: str,
    symbol: str,
    side: str,
    order_type: str,
    qty: float,
    price: float,
    signal_id: Optional[str] = None,
    sub_uid: Optional[str] = None,
    owner: Optional[str] = None,
    strategy_role: Optional[str] = None,
    exit_profile: Optional[str] = None,
    reduce_only: Optional[bool] = None,
    post_only: Optional[bool] = None,
    extra: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
) -> None:
    """
    Convenience wrapper to log an order when you place it.
    """
    if ts_ms is None:
        ts_ms = _now_ms()

    payload: OrderLog = {
        "order_id": order_id,
        "ts_ms": ts_ms,
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "qty": float(qty),
        "price": float(price),
        "signal_id": signal_id,
        "sub_uid": sub_uid,
        "owner": owner,
        "strategy_role": strategy_role,
        "exit_profile": exit_profile,
        "reduce_only": reduce_only,
        "post_only": post_only,
        "extra": extra or {},
    }

    _ai_store.log_order(payload)


# ---------- TRADES (CLOSED ROUND-TRIPS) ----------

def log_trade_summary_basic(
    *,
    trade_id: str,
    sub_uid: str,
    symbol: str,
    side: str,
    opened_ts_ms: int,
    closed_ts_ms: int,
    outcome: str,
    r_multiple: float,
    realized_pnl: float,
    signal_id: Optional[str] = None,
    owner: Optional[str] = None,
    strategy_role: Optional[str] = None,
    entry_price: Optional[float] = None,
    exit_price: Optional[float] = None,
    max_favorable_excursion_r: Optional[float] = None,
    max_adverse_excursion_r: Optional[float] = None,
    holding_ms: Optional[int] = None,
    exit_reason: Optional[str] = None,
    regime_at_entry: Optional[str] = None,
    regime_at_exit: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Convenience wrapper to log a fully closed trade.

    Typically called from:
    - TP/SL manager when a position hits TP / SL
    - Trade journal when you manually close a trade
    """
    payload: TradeSummaryLog = {
        "trade_id": trade_id,
        "sub_uid": sub_uid,
        "symbol": symbol,
        "side": side,
        "opened_ts_ms": opened_ts_ms,
        "closed_ts_ms": closed_ts_ms,
        "outcome": outcome,
        "r_multiple": float(r_multiple),
        "realized_pnl": float(realized_pnl),
        "signal_id": signal_id,
        "owner": owner,
        "strategy_role": strategy_role,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "max_favorable_excursion_r": max_favorable_excursion_r,
        "max_adverse_excursion_r": max_adverse_excursion_r,
        "holding_ms": holding_ms,
        "exit_reason": exit_reason,
        "regime_at_entry": regime_at_entry,
        "regime_at_exit": regime_at_exit,
        "extra": extra or {},
    }

    _ai_store.log_trade_summary(payload)
