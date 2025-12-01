#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Execution (WS-first helpers)

Purpose
-------
Provide a small, consistent surface for higher-level logic (AI, scripts)
to:

    • Open positions sized by % of equity (notional-based).
    • Flatten a single symbol.
    • Flatten all symbols.
    • List open symbols.

All of this is built on top of app.core.flashback_common and is designed
to be:

    • WS-first for pricing (via last_price_ws_first / spread_bps_ws).
    • Bybit v5 REST for actual order placement.
    • Safe, with spread caps and Telegram notifications.

This module intentionally does NOT manage TP/SL; that is the job of
tp_sl_manager, which reacts to positions via position_bus.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from app.core.flashback_common import (
    send_tg,
    alert_bot_error,
    record_heartbeat,
    get_equity_usdt,
    list_open_positions,
    qty_from_pct,
    last_price,
    last_price_ws_first,
    spread_bps_ws,
    place_market_entry,
    reduce_only_market,
    cancel_all,
)


CATEGORY = "linear"
QUOTE = "USDT"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _side_to_bybit_order_side(position_side: str) -> str:
    """
    Convert Bybit position side to opposite order side for flatten operations.

    position_side: "Buy" | "Sell"
    """
    s = (position_side or "").strip().lower()
    if s == "buy":
        return "Sell"
    if s == "sell":
        return "Buy"
    raise ValueError(f"Unknown position side: {position_side!r}")


def _normalize_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        raise ValueError("symbol is required")
    return s


def _log(msg: str) -> None:
    """
    Best-effort Telegram log for execution events.
    """
    try:
        send_tg(msg)
    except Exception:
        # Don't ever let logging kill execution
        pass


# ---------------------------------------------------------------------------
# Public: list open symbols
# ---------------------------------------------------------------------------

def list_open_symbols() -> List[str]:
    """
    Return a sorted list of symbols with non-zero linear positions.
    """
    try:
        rows = list_open_positions(category=CATEGORY, settle_coin=QUOTE)
    except Exception as e:
        alert_bot_error("execution_ws", f"list_open_positions error: {e}", "ERROR")
        return []

    syms: Set[str] = set()
    for p in rows:
        try:
            size = Decimal(str(p.get("size", "0")))
        except Exception:
            size = Decimal("0")
        if size > 0:
            sym = str(p.get("symbol") or "").strip().upper()
            if sym:
                syms.add(sym)

    return sorted(syms)


# ---------------------------------------------------------------------------
# Public: open position (WS-first pricing)
# ---------------------------------------------------------------------------

def open_position_ws_first(
    symbol: str,
    side: str,
    risk_pct_notional: Decimal,
    max_spread_bps: Optional[Decimal] = None,
    leverage_override: Optional[int] = None,
    notify: bool = True,
) -> Dict[str, Any]:
    """
    Open a position sized by % of equity (notional-based).

    Args:
        symbol              : e.g. "BTCUSDT"
        side                : "LONG" | "SHORT"
        risk_pct_notional   : Decimal percent of equity, e.g. Decimal("15")
        max_spread_bps      : optional cap on bid/ask spread in basis points.
                              If spread exceeds this, no trade is placed.
        leverage_override   : optional leverage to set before entry
        notify              : if True, send Telegram message

    Returns a dict with:
        {
          "ok": bool,
          "error": str or None,
          "symbol": str,
          "side": "LONG"/"SHORT",
          "qty": str or None,
          "px_ref": str or None,
          "spread_bps": str or None,
        }
    """
    record_heartbeat("execution_ws")

    sym = _normalize_symbol(symbol)
    side_norm = str(side or "").strip().upper()
    if side_norm not in ("LONG", "SHORT"):
        raise ValueError("side must be 'LONG' or 'SHORT'")

    # 1) Optionally check spread from WS orderbook
    spread_val: Optional[Decimal] = None
    if max_spread_bps is not None and max_spread_bps > 0:
        try:
            spread_val = spread_bps_ws(sym)
        except Exception as e:
            alert_bot_error("execution_ws", f"{sym} spread_bps_ws error: {e}", "WARN")
            spread_val = None

        if spread_val is not None and spread_val > max_spread_bps:
            msg = (
                f"Spread too wide for {sym}: {spread_val:.2f} bps > "
                f"cap {max_spread_bps:.2f} bps. Skipping entry."
            )
            _log(f"🚫 {msg}")
            return {
                "ok": False,
                "error": msg,
                "symbol": sym,
                "side": side_norm,
                "qty": None,
                "px_ref": None,
                "spread_bps": f"{spread_val:.4f}",
            }

    # 2) Determine equity and qty from notional %
    try:
        equity = get_equity_usdt()
    except Exception as e:
        alert_bot_error("execution_ws", f"get_equity_usdt error: {e}", "ERROR")
        return {
            "ok": False,
            "error": f"get_equity_usdt error: {e}",
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    if equity <= 0:
        msg = "Equity is zero or negative; cannot size position."
        _log(f"🚫 {msg}")
        return {
            "ok": False,
            "error": msg,
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    try:
        qty = qty_from_pct(sym, equity, risk_pct_notional)
    except Exception as e:
        alert_bot_error("execution_ws", f"{sym} qty_from_pct error: {e}", "ERROR")
        return {
            "ok": False,
            "error": f"qty_from_pct error: {e}",
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    if qty <= 0:
        msg = f"Computed qty <= 0 for {sym} at {risk_pct_notional}% of equity."
        _log(f"🚫 {msg}")
        return {
            "ok": False,
            "error": msg,
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    # 3) Reference price (WS-first, fallback to REST ticker)
    try:
        px_ref = last_price_ws_first(sym)
        if px_ref <= 0:
            px_ref = last_price(sym)
    except Exception:
        try:
            px_ref = last_price(sym)
        except Exception:
            px_ref = Decimal("0")

    # 4) Place market order
    try:
        res = place_market_entry(
            symbol=sym,
            side=side_norm,
            qty=qty,
            leverage=leverage_override,
        )
    except Exception as e:
        alert_bot_error("execution_ws", f"{sym} place_market_entry error: {e}", "ERROR")
        return {
            "ok": False,
            "error": f"place_market_entry error: {e}",
            "symbol": sym,
            "side": side_norm,
            "qty": str(qty),
            "px_ref": str(px_ref),
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    if notify:
        _log(
            f"✅ OPEN {sym} {side_norm} | "
            f"risk={risk_pct_notional}% | qty={qty} | "
            f"px_ref={px_ref} | "
            f"spread_bps={spread_val:.2f}" if spread_val is not None else
            f"✅ OPEN {sym} {side_norm} | "
            f"risk={risk_pct_notional}% | qty={qty} | "
            f"px_ref={px_ref}"
        )

    return {
        "ok": True,
        "error": None,
        "symbol": sym,
        "side": side_norm,
        "qty": str(qty),
        "px_ref": str(px_ref),
        "spread_bps": str(spread_val) if spread_val is not None else None,
        "raw": res,
    }


# ---------------------------------------------------------------------------
# Public: flatten a single symbol
# ---------------------------------------------------------------------------

def flatten_symbol_ws_first(
    symbol: str,
    notify: bool = True,
) -> Dict[str, Any]:
    """
    Flatten all linear positions for a given symbol via reduce-only market.

    Steps:
      1) List positions via list_open_positions().
      2) For matching symbol:
            • send reduce-only market order to close size.
      3) Optionally cancel open orders (TP/SL) for that symbol.
         (tp_sl_manager will also adapt.)

    Returns:
      {
        "ok": bool,
        "error": str or None,
        "symbol": str,
        "flattened_qty": str,
      }
    """
    record_heartbeat("execution_ws")

    sym = _normalize_symbol(symbol)

    try:
        rows = list_open_positions(category=CATEGORY, settle_coin=QUOTE)
    except Exception as e:
        alert_bot_error("execution_ws", f"{sym} list_open_positions error: {e}", "ERROR")
        return {
            "ok": False,
            "error": f"list_open_positions error: {e}",
            "symbol": sym,
            "flattened_qty": "0",
        }

    total_flattened = Decimal("0")
    errors: List[str] = []

    for p in rows:
        p_sym = str(p.get("symbol") or "").strip().upper()
        if p_sym != sym:
            continue

        try:
            size = Decimal(str(p.get("size", "0")))
        except Exception:
            size = Decimal("0")

        if size <= 0:
            continue

        side = str(p.get("side") or "").strip()
        try:
            # We can ignore explicit side mapping here because reduce_only_market
            # already figures it out from side_now, but we keep the helper for clarity.
            res = reduce_only_market(sym, side, size)
            total_flattened += size
        except Exception as e:
            msg = f"{sym} reduce_only_market error: {e}"
            alert_bot_error("execution_ws", msg, "ERROR")
            errors.append(str(e))

    # Best-effort cancel open orders for that symbol
    try:
        cancel_all(sym)
    except Exception as e:
        alert_bot_error("execution_ws", f"{sym} cancel_all error: {e}", "WARN")

    if notify:
        if total_flattened > 0:
            _log(f"🧹 FLATTEN {sym} | qty={total_flattened} | errors={len(errors)}")
        else:
            _log(f"ℹ️ FLATTEN {sym} requested but no open position found.")

    if errors:
        return {
            "ok": total_flattened > 0,
            "error": "; ".join(errors),
            "symbol": sym,
            "flattened_qty": str(total_flattened),
        }

    return {
        "ok": True,
        "error": None,
        "symbol": sym,
        "flattened_qty": str(total_flattened),
    }


# ---------------------------------------------------------------------------
# Public: flatten all symbols (convenience)
# ---------------------------------------------------------------------------

def flatten_all_ws_first(notify: bool = True) -> Dict[str, Any]:
    """
    Flatten all linear symbols with open positions.

    Returns:
      {
        "ok": bool,
        "results": {
          "BTCUSDT": {...},
          "ETHUSDT": {...},
          ...
        }
      }
    """
    syms = list_open_symbols()
    results: Dict[str, Any] = {}

    for sym in syms:
        res = flatten_symbol_ws_first(sym, notify=notify)
        results[sym] = res

    if notify:
        _log(f"🧹 FLATTEN_ALL requested for {len(syms)} symbols.")

    return {
        "ok": True,
        "results": results,
    }


if __name__ == "__main__":
    # Simple manual smoke test (no actual trading unless you call the functions).
    print("execution_ws module loaded. Use from other scripts; no CLI here.")
