#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — State Hub (WS-first AI/Signal Harness)

Purpose
-------
Provide a SINGLE place for higher-level logic (AI, signal engines, guards)
to query the current trading state without caring about:

  - REST vs WebSocket
  - Which file holds what snapshot
  - How tier/equity/positions are computed

This module pulls from:
  - app.core.position_bus   (positions, per ACCOUNT_LABEL)
  - app.core.market_bus     (orderbook + trades, WS-fed)
  - app.core.flashback_common (equity, tier, WS-first prices)

Key entry points:
  - get_symbol_state(symbol: str) -> dict
  - get_portfolio_state(symbols: Optional[List[str]] = None) -> dict

Design goals:
  - Read-only, no side effects.
  - WS-first, with graceful degradation when WS data is missing.
  - JSON-serializable output so it can be directly fed into AI models.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Core account / pricing primitives
from app.core.flashback_common import (
    get_equity_usdt,
    tier_from_equity,
    last_price_ws_first,
    best_bid_ask_ws_first,
    spread_bps_ws,
)

# Positions via position_bus (WS-first + REST fallback for MAIN)
from app.core.position_bus import (
    get_positions_for_current_label,
    get_position_map_for_label,
)

# Market data via market_bus (WS-fed snapshots)
from app.core import market_bus


CATEGORY = "linear"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _dec_or_none(val: Any) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        d = Decimal(str(val))
    except Exception:
        return None
    return d


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except Exception:
        return default


def _account_tier_snapshot() -> Dict[str, Any]:
    """
    Return a normalized account tier/equity snapshot.

    {
      "equity_usdt": "123.45",
      "tier": 1,
      "level": 2
    }
    """
    eq = get_equity_usdt()
    tier, level = tier_from_equity(eq)
    return {
        "equity_usdt": str(eq),
        "tier": tier,
        "level": level,
    }


def _positions_snapshot_map() -> Dict[str, Dict[str, Any]]:
    """
    Return { symbol -> position_row } for current ACCOUNT_LABEL.
    """
    return get_position_map_for_label(
        label=None,         # position_bus will use ACCOUNT_LABEL
        category=CATEGORY,
        max_age_seconds=None,
        allow_rest_fallback=True,
    )


def _open_positions_list() -> List[Dict[str, Any]]:
    """
    Return list of normalized open positions for current ACCOUNT_LABEL:

    [
      {
        "symbol": "BTCUSDT",
        "side": "Buy"/"Sell",
        "size": "0.001",
        "avg_price": "12345.6",
        "stop_loss": "0" or None or "12300.0",
        "raw": {...original row...}
      },
      ...
    ]
    """
    rows = get_positions_for_current_label(
        category=CATEGORY,
        max_age_seconds=None,
        allow_rest_fallback=True,
    )
    out: List[Dict[str, Any]] = []

    for p in rows:
        symbol = str(p.get("symbol") or "").upper()
        if not symbol:
            continue

        size = _dec_or_none(p.get("size"))
        if size is None or size <= 0:
            continue

        avg_price = _dec_or_none(p.get("avgPrice"))
        side = p.get("side") or ""
        side = str(side)

        sl_raw = (
            p.get("stopLoss")
            or p.get("stopLossPrice")
            or p.get("slPrice")
            or p.get("stop_loss")
        )
        sl_val = _dec_or_none(sl_raw)

        out.append({
            "symbol": symbol,
            "side": side,
            "size": str(size),
            "avg_price": str(avg_price) if avg_price is not None else None,
            "stop_loss": str(sl_val) if sl_val is not None else None,
            "raw": p,
        })

    return out


def _ws_bus_meta() -> Dict[str, Any]:
    """
    Basic freshness metadata for WS-fed buses:

    {
      "orderbook_updated_ms": 1763752000123,
      "trades_updated_ms": 1763752000456
    }
    """
    return {
        "orderbook_updated_ms": _safe_int(market_bus.orderbook_bus_updated_ms(), 0),
        "trades_updated_ms": _safe_int(market_bus.trades_bus_updated_ms(), 0),
    }


# ---------------------------------------------------------------------------
# Symbol-level state
# ---------------------------------------------------------------------------

def get_symbol_state(symbol: str, *, include_trades: bool = True, trades_limit: int = 50) -> Dict[str, Any]:
    """
    Return a WS-first snapshot for a single symbol.

    Output shape (all values JSON-serializable):

    {
      "symbol": "BTCUSDT",
      "price": {
        "last_ws_first": "12345.67",
        "best_bid": "12345.50" or null,
        "best_ask": "12345.80" or null,
        "mid": "12345.65" or null,
        "spread_abs": "0.30" or null,
        "spread_bps": "2.4" or null
      },
      "position": {
        "has_position": true/false,
        "side": "Buy"/"Sell"/null,
        "size": "0.001" or null,
        "avg_price": "12300.0" or null,
        "stop_loss": "12200.0" or null,
        "raw": {...} or null
      },
      "trades": {
        "count": 12,
        "recent": [ {...}, ... ]   # up to trades_limit, may be empty
      },
      "meta": {
        "orderbook_ts_ms": 1763752000000,
        "trades_ts_ms": 1763751999000
      }
    }
    """
    sym = symbol.strip().upper()

    # --- price block ---
    last_px = last_price_ws_first(sym)
    bid, ask = best_bid_ask_ws_first(sym)

    # market_bus.mid_price / spread may return Decimal or None
    try:
        mid = market_bus.mid_price(sym)
    except Exception:
        mid = None

    try:
        spread_abs = market_bus.spread(sym)
    except Exception:
        spread_abs = None

    s_bps = spread_bps_ws(sym)

    price_block: Dict[str, Any] = {
        "last_ws_first": str(last_px) if last_px is not None else None,
        "best_bid": str(bid) if bid is not None else None,
        "best_ask": str(ask) if ask is not None else None,
        "mid": str(mid) if mid is not None else None,
        "spread_abs": str(spread_abs) if spread_abs is not None else None,
        "spread_bps": str(s_bps) if s_bps is not None else None,
    }

    # --- position block ---
    pos_map = _positions_snapshot_map()
    p = pos_map.get(sym)
    if p:
        size = _dec_or_none(p.get("size"))
        avg_price = _dec_or_none(p.get("avgPrice"))
        sl_raw = (
            p.get("stopLoss")
            or p.get("stopLossPrice")
            or p.get("slPrice")
            or p.get("stop_loss")
        )
        sl_val = _dec_or_none(sl_raw)

        pos_block: Dict[str, Any] = {
            "has_position": size is not None and size > 0,
            "side": p.get("side"),
            "size": str(size) if size is not None else None,
            "avg_price": str(avg_price) if avg_price is not None else None,
            "stop_loss": str(sl_val) if sl_val is not None else None,
            "raw": p,
        }
    else:
        pos_block = {
            "has_position": False,
            "side": None,
            "size": None,
            "avg_price": None,
            "stop_loss": None,
            "raw": None,
        }

    # --- trades block ---
    trades_block: Dict[str, Any]
    trades_recent: List[Dict[str, Any]] = []
    if include_trades and trades_limit > 0:
        try:
            trades_recent = market_bus.get_recent_trades(sym, limit=trades_limit)
        except Exception:
            trades_recent = []
    trades_block = {
        "count": len(trades_recent),
        "recent": trades_recent,
    }

    # --- meta block ---
    try:
        ob_entry = market_bus.get_orderbook_snapshot(sym)
        ob_ts = _safe_int(ob_entry.get("ts_ms", 0), 0)
    except Exception:
        ob_ts = 0

    try:
        trades_snap = market_bus.load_trades_bus()
        trades_sym = (trades_snap.get("symbols") or {}).get(sym) or {}
        tr_ts = _safe_int(trades_sym.get("ts_ms", 0), 0)
    except Exception:
        tr_ts = 0

    meta_block = {
        "orderbook_ts_ms": ob_ts,
        "trades_ts_ms": tr_ts,
    }

    return {
        "symbol": sym,
        "price": price_block,
        "position": pos_block,
        "trades": trades_block,
        "meta": meta_block,
    }


# ---------------------------------------------------------------------------
# Portfolio-level state
# ---------------------------------------------------------------------------

def get_portfolio_state(symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Return a WS-first snapshot for the entire account (current ACCOUNT_LABEL).

    If `symbols` is provided, we build symbol_state only for that watchlist.
    Otherwise we only include symbol_state for currently-open positions.

    Output shape:

    {
      "account": {
        "equity_usdt": "123.45",
        "tier": 1,
        "level": 2
      },
      "ws_meta": {
        "orderbook_updated_ms": 1763752000123,
        "trades_updated_ms": 1763752000456
      },
      "positions": [
        ...normalized open positions from _open_positions_list()...
      ],
      "symbols": {
        "BTCUSDT": { <get_symbol_state("BTCUSDT")> },
        "ETHUSDT": { ... },
        ...
      }
    }
    """
    # Account / tier
    account_block = _account_tier_snapshot()

    # WS freshness meta
    ws_meta_block = _ws_bus_meta()

    # Positions
    open_positions = _open_positions_list()

    # Symbols to include in symbol_state
    if symbols is not None:
        watch = sorted({s.strip().upper() for s in symbols if s.strip()})
    else:
        # Only track symbols we actually have positions on
        watch = sorted({p["symbol"] for p in open_positions})

    symbols_block: Dict[str, Any] = {}
    for sym in watch:
        try:
            symbols_block[sym] = get_symbol_state(sym, include_trades=False)
        except Exception as e:
            # If a single symbol fails, don't kill the whole snapshot.
            symbols_block[sym] = {
                "symbol": sym,
                "error": str(e),
            }

    return {
        "account": account_block,
        "ws_meta": ws_meta_block,
        "positions": open_positions,
        "symbols": symbols_block,
    }
