#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Router (WS-first, profile-aware)

Purpose
-------
Define a strict, validated contract between:

    • AI / strategy engines (propose actions as dicts), and
    • Execution core (execution_ws.py).

Supported actions:
    - OPEN
        {
          "type": "OPEN",
          "symbol": "BTCUSDT",
          "side": "LONG" | "SHORT",
          "risk_pct_notional": "15.0",        # str or number
          "max_spread_bps": "4.0",            # optional
          "leverage_override": 25             # optional int
        }

    - FLATTEN
        {
          "type": "FLATTEN",
          "symbol": "BTCUSDT"
        }

    - FLATTEN_ALL
        {
          "type": "FLATTEN_ALL"
        }

    - NOP
        {
          "type": "NOP"
        }

Config is pulled from app.core.ai_profile per ACCOUNT_LABEL:

    - allowed_symbols
    - require_whitelist
    - max_notional_pct
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional

from app.core.flashback_common import (
    send_tg,
    alert_bot_error,
    record_heartbeat,
)

from app.core.execution_ws import (
    open_position_ws_first,
    flatten_symbol_ws_first,
    list_open_symbols,
)

from app.core.ai_profile import get_current_ai_profile


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_decimal(val: Any, field: str) -> Decimal:
    try:
        return Decimal(str(val))
    except Exception:
        raise ValueError(f"Invalid decimal for {field}: {val!r}")


def _normalize_symbol(sym: Any) -> str:
    s = str(sym or "").strip().upper()
    if not s:
        raise ValueError("symbol is required and cannot be empty")
    return s


def _normalize_side(side: Any) -> str:
    s = str(side or "").strip().upper()
    if s not in ("LONG", "SHORT"):
        raise ValueError("side must be 'LONG' or 'SHORT'")
    return s


def _validate_symbol_whitelist(symbol: str, profile: Dict[str, Any]) -> None:
    require = bool(profile.get("require_whitelist", False))
    allowed = profile.get("allowed_symbols") or []
    if not require:
        return
    if allowed and symbol not in allowed:
        raise ValueError(
            f"symbol '{symbol}' is not in AI allowed_symbols for account "
            f"{profile.get('account_label')}"
        )


def _validate_notional_pct(pct: Decimal, profile: Dict[str, Any]) -> None:
    if pct <= 0:
        raise ValueError("risk_pct_notional must be > 0")
    max_pct = profile.get("max_notional_pct")
    if isinstance(max_pct, Decimal) and max_pct > 0 and pct > max_pct:
        raise ValueError(
            f"risk_pct_notional {pct}% exceeds AI profile max_notional_pct={max_pct}%"
        )


def _normalize_open_action(payload: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    symbol = _normalize_symbol(payload.get("symbol"))
    side = _normalize_side(payload.get("side"))
    risk_pct_notional = _to_decimal(payload.get("risk_pct_notional"), "risk_pct_notional")

    _validate_symbol_whitelist(symbol, profile)
    _validate_notional_pct(risk_pct_notional, profile)

    max_spread_raw = payload.get("max_spread_bps")
    max_spread_bps: Optional[Decimal]
    if max_spread_raw is None:
        max_spread_bps = None
    else:
        max_spread_bps = _to_decimal(max_spread_raw, "max_spread_bps")
        if max_spread_bps <= 0:
            max_spread_bps = None

    lev_raw = payload.get("leverage_override")
    lev: Optional[int]
    if lev_raw is None:
        lev = None
    else:
        try:
            lev_int = int(lev_raw)
            lev = lev_int if lev_int > 0 else None
        except Exception:
            raise ValueError(f"Invalid leverage_override: {lev_raw!r}")

    return {
        "type": "OPEN",
        "symbol": symbol,
        "side": side,
        "risk_pct_notional": risk_pct_notional,
        "max_spread_bps": max_spread_bps,
        "leverage_override": lev,
    }


def _normalize_flatten_action(payload: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    symbol = _normalize_symbol(payload.get("symbol"))
    _validate_symbol_whitelist(symbol, profile)
    return {
        "type": "FLATTEN",
        "symbol": symbol,
    }


def _normalize_flatten_all(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "FLATTEN_ALL",
    }


def _normalize_nop(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "NOP",
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_ai_action(raw_action: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate + normalize a raw AI action dict, using the current AI profile.

    Returns a canonical dict (type + normalized fields) or raises ValueError.
    """
    if not isinstance(raw_action, dict):
        raise ValueError("AI action must be a dict")

    profile = get_current_ai_profile()

    action_type = str(raw_action.get("type") or "").strip().upper()
    if not action_type:
        raise ValueError("AI action missing 'type' field")

    payload = dict(raw_action)  # shallow copy

    if action_type == "OPEN":
        return _normalize_open_action(payload, profile)
    if action_type == "FLATTEN":
        return _normalize_flatten_action(payload, profile)
    if action_type == "FLATTEN_ALL":
        return _normalize_flatten_all(payload)
    if action_type == "NOP":
        return _normalize_nop(payload)

    raise ValueError(f"Unsupported AI action type: {action_type!r}")


def apply_ai_action(raw_action: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main entry point: validate, normalize, and apply a SINGLE AI action.

    Returns a structured result:
      {
        "ok": bool,
        "error": str or None,
        "normalized": { ...normalized_action... } or None,
        "result": any  # raw execution result or structured summary
      }
    """
    record_heartbeat("ai_action_router")

    try:
        normalized = normalize_ai_action(raw_action)
    except Exception as e:
        msg = f"AI action validation failed: {e}"
        alert_bot_error("ai_action_router", msg, "WARN")
        return {
            "ok": False,
            "error": msg,
            "normalized": None,
            "result": None,
        }

    a_type = normalized["type"]

    # NOP: explicit no-op from AI
    if a_type == "NOP":
        return {
            "ok": True,
            "error": None,
            "normalized": normalized,
            "result": {"noop": True},
        }

    try:
        if a_type == "OPEN":
            res = open_position_ws_first(
                symbol=normalized["symbol"],
                side=normalized["side"],
                risk_pct_notional=normalized["risk_pct_notional"],
                max_spread_bps=normalized["max_spread_bps"],
                leverage_override=normalized["leverage_override"],
                notify=True,
            )
            return {
                "ok": True,
                "error": None,
                "normalized": normalized,
                "result": res,
            }

        if a_type == "FLATTEN":
            res = flatten_symbol_ws_first(
                symbol=normalized["symbol"],
                notify=True,
            )
            return {
                "ok": True,
                "error": None,
                "normalized": normalized,
                "result": res,
            }

        if a_type == "FLATTEN_ALL":
            results: Dict[str, Any] = {}
            symbols = list_open_symbols()
            for sym in symbols:
                try:
                    r = flatten_symbol_ws_first(sym, notify=True)
                    results[sym] = {"ok": True, "result": r}
                except Exception as e_flat:
                    err_msg = f"Flatten {sym} failed: {e_flat}"
                    alert_bot_error("ai_action_router", err_msg, "ERROR")
                    results[sym] = {"ok": False, "error": str(e_flat)}
            return {
                "ok": True,
                "error": None,
                "normalized": normalized,
                "result": results,
            }

        raise RuntimeError(f"Unknown normalized action type {a_type!r}")

    except Exception as e:
        msg = f"AI action execution error: {e}"
        alert_bot_error("ai_action_router", msg, "ERROR")
        try:
            send_tg(f"❌ AI action failed: {msg}")
        except Exception:
            pass
        return {
            "ok": False,
            "error": msg,
            "normalized": normalized,
            "result": None,
        }


def apply_ai_actions(raw_actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convenience helper: apply a LIST of AI actions sequentially.

    Returns a list of apply_ai_action(...) results in the same order.
    """
    if not isinstance(raw_actions, list):
        raise ValueError("AI actions must be a list of dicts")

    out: List[Dict[str, Any]] = []
    for a in raw_actions:
        try:
            res = apply_ai_action(a)
        except Exception as e:
            msg = f"Unexpected error applying action {a}: {e}"
            alert_bot_error("ai_action_router", msg, "ERROR")
            out.append({
                "ok": False,
                "error": msg,
                "normalized": None,
                "result": None,
            })
        else:
            out.append(res)
    return out
