#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Market Data Bus v1.0

Purpose
-------
Unified, read-only interface for market data snapshots created by
ws_switchboard.py.

Provides:
    get_l2_snapshot(symbol)
    get_recent_trades(symbol, lookback_ms=2000)
    get_mid_price(symbol)
    get_top_of_book(symbol)

Data sources (JSON files updated by ws_switchboard):
    state/orderbook_bus.json
    state/trades_bus.json

This module performs:
    • Fast disk reads via orjson
    • Minimal validation
    • Symbol normalization
    • Safe defaults (None instead of crash)

Design:
    All bots (including upcoming HFT_MM) should use THIS module instead
    of touching JSON files directly.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
except Exception:
    class _DummySettings:
        ROOT: Path = Path(__file__).resolve().parents[2]
    settings = _DummySettings()

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"

ORDERBOOK_PATH: Path = STATE_DIR / "orderbook_bus.json"
TRADES_PATH: Path = STATE_DIR / "trades_bus.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Core JSON loaders
# ---------------------------------------------------------------------------

def _load_orderbook() -> Dict[str, Any]:
    if not ORDERBOOK_PATH.exists():
        return {"symbols": {}, "version": 1, "updated_ms": 0}

    try:
        data = orjson.loads(ORDERBOOK_PATH.read_bytes())
        if not isinstance(data, dict):
            raise ValueError("orderbook_bus root is not a dict")
        return data
    except Exception:
        return {"symbols": {}, "version": 1, "updated_ms": 0}


def _load_trades() -> Dict[str, Any]:
    if not TRADES_PATH.exists():
        return {"symbols": {}, "version": 1, "updated_ms": 0}

    try:
        data = orjson.loads(TRADES_PATH.read_bytes())
        if not isinstance(data, dict):
            raise ValueError("trades_bus root is not a dict")
        return data
    except Exception:
        return {"symbols": {}, "version": 1, "updated_ms": 0}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_l2_snapshot(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Return the current L2 snapshot for a symbol:
        {
            "bids": [[price, size], ...],
            "asks": [[price, size], ...],
            "ts_ms": 1700000000000
        }

    If not available -> return None.
    """
    symbol = symbol.upper()
    data = _load_orderbook()
    block = data.get("symbols", {}).get(symbol)
    if not isinstance(block, dict):
        return None
    return block


def get_top_of_book(symbol: str) -> Optional[Dict[str, float]]:
    """
    Return:
        {
            "bid_px": float,
            "bid_sz": float,
            "ask_px": float,
            "ask_sz": float
        }
    or None if incomplete.
    """
    snap = get_l2_snapshot(symbol)
    if not snap:
        return None

    bids = snap.get("bids") or []
    asks = snap.get("asks") or []

    if not bids or not asks:
        return None

    try:
        bid_px, bid_sz = float(bids[0][0]), float(bids[0][1])
        ask_px, ask_sz = float(asks[0][0]), float(asks[0][1])
    except Exception:
        return None

    return {
        "bid_px": bid_px,
        "bid_sz": bid_sz,
        "ask_px": ask_px,
        "ask_sz": ask_sz,
    }


def get_mid_price(symbol: str) -> Optional[float]:
    """
    Return mid price = (best bid + best ask) / 2.
    """
    tob = get_top_of_book(symbol)
    if not tob:
        return None
    return (tob["bid_px"] + tob["ask_px"]) / 2.0


def get_recent_trades(symbol: str, lookback_ms: int = 2000) -> List[Dict[str, Any]]:
    """
    Return list of trades in the last `lookback_ms`.

    Each trade row typically looks like:
        {
            "p": "price",
            "s": "size",
            "T": 1700000000000,
            "S": "Buy" / "Sell"
        }
    (keys vary slightly depending on Bybit format)
    """
    now_ms = _now_ms()
    symbol = symbol.upper()

    data = _load_trades()
    sym_block = data.get("symbols", {}).get(symbol)
    if not isinstance(sym_block, dict):
        return []

    trades = sym_block.get("trades", [])
    if not isinstance(trades, list):
        return []

    cutoff = now_ms - lookback_ms
    out: List[Dict[str, Any]] = []

    for t in trades:
        if not isinstance(t, dict):
            continue

        # Try multiple timestamp fields
        ts = (
            t.get("T")
            or t.get("ts")
            or t.get("time")
            or t.get("trade_time")
        )
        try:
            ts = int(ts)
        except Exception:
            continue

        if ts >= cutoff:
            out.append(t)

    return out


# ---------------------------------------------------------------------------
# Convenience bundled snapshot
# ---------------------------------------------------------------------------

def get_snapshot(symbol: str, trades_ms: int = 2000) -> Dict[str, Any]:
    """
    Unified snapshot for convenience:
        {
            "symbol": "BTCUSDT",
            "mid": float or None,
            "tob": {...} or None,
            "l2": {...} or None,
            "trades": [ ... ]
        }
    """
    symbol = symbol.upper()
    return {
        "symbol": symbol,
        "mid": get_mid_price(symbol),
        "tob": get_top_of_book(symbol),
        "l2": get_l2_snapshot(symbol),
        "trades": get_recent_trades(symbol, lookback_ms=trades_ms),
    }
