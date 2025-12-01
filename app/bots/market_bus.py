# app/core/market_bus.py
# Flashback — Market Bus (WS-fed orderbook + trades snapshot reader)
#
# Purpose
# -------
# Read-only interface over:
#   - state/orderbook_bus.json  (written by ws_switchboard)
#   - state/trades_bus.json     (written by ws_switchboard)
#
# This lets bots query:
#   - best bid/ask / mid / spread
#   - raw L2 snapshots per symbol
#   - recent public trades per symbol
#
# WITHOUT hammering REST endpoints.
#
# File structures (as written by ws_switchboard):
#
# orderbook_bus.json:
#   {
#     "version": 1,
#     "updated_ms": 1763752000123,
#     "symbols": {
#       "BTCUSDT": {
#         "bids": [["price","qty"], ...],
#         "asks": [["price","qty"], ...],
#         "ts_ms": 1763751999000
#       },
#       ...
#     }
#   }
#
# trades_bus.json:
#   {
#     "version": 1,
#     "updated_ms": 1763752000123,
#     "symbols": {
#       "BTCUSDT": {
#         "trades": [
#           { "p": "price", "v": "qty", "T": 1763751999000, ... },
#           ...
#         ]
#       },
#       ...
#     }
#   }

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson

# Tolerant import of settings / ROOT
try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        ROOT: Path = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

ORDERBOOK_PATH: Path = STATE_DIR / "orderbook_bus.json"
TRADES_PATH: Path = STATE_DIR / "trades_bus.json"


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if not path.exists():
            return None
        raw = path.read_bytes()
        if not raw:
            return None
        data = orjson.loads(raw)
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


# ---------------------------------------------------------------------------
# Orderbook bus accessors
# ---------------------------------------------------------------------------

def _empty_orderbook_snapshot() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_ms": 0,
        "symbols": {},
    }


def load_orderbook_bus() -> Dict[str, Any]:
    """
    Return the entire orderbook_bus snapshot dict (never None).
    """
    snap = _load_json(ORDERBOOK_PATH)
    if snap is None:
        return _empty_orderbook_snapshot()

    if "symbols" not in snap or not isinstance(snap["symbols"], dict):
        snap["symbols"] = {}
    if "version" not in snap:
        snap["version"] = 1
    if "updated_ms" not in snap:
        snap["updated_ms"] = 0
    return snap


def orderbook_bus_updated_ms() -> int:
    """
    Return the last updated_ms reported in orderbook_bus.json or 0.
    """
    snap = _load_json(ORDERBOOK_PATH)
    if not snap:
        return 0
    try:
        return int(snap.get("updated_ms", 0))
    except Exception:
        return 0


def get_orderbook_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Return a per-symbol orderbook snapshot:

      {
        "symbol": "BTCUSDT",
        "bids": [["price","qty"], ...],
        "asks": [["price","qty"], ...],
        "ts_ms": <int> or 0,
        "updated_ms": <int> (file updated_ms)
      }

    If symbol not present, returns an empty shell with no bids/asks.
    """
    sym = _normalize_symbol(symbol)
    snap = load_orderbook_bus()
    symbols = snap.get("symbols", {}) or {}
    entry = symbols.get(sym) or {}

    bids = entry.get("b") or entry.get("bids") or []
    asks = entry.get("a") or entry.get("asks") or []
    ts_ms = entry.get("ts") or entry.get("ts_ms") or 0

    try:
        ts_ms_int = int(ts_ms)
    except Exception:
        ts_ms_int = 0

    return {
        "symbol": sym,
        "bids": bids if isinstance(bids, list) else [],
        "asks": asks if isinstance(asks, list) else [],
        "ts_ms": ts_ms_int,
        "updated_ms": int(snap.get("updated_ms", 0)) if isinstance(snap.get("updated_ms"), (int, float)) else 0,
    }


def _best_from_side(levels: List[List[Any]], is_bid: bool) -> Optional[Tuple[Decimal, Decimal]]:
    """
    Given L2 levels [["price","qty"], ...], return (best_price, qty) or None.
    """
    if not levels:
        return None

    best_price: Optional[Decimal] = None
    best_qty: Decimal = Decimal("0")

    for lvl in levels:
        if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
            continue
        try:
            px = Decimal(str(lvl[0]))
            qty = Decimal(str(lvl[1]))
        except Exception:
            continue

        if best_price is None:
            best_price = px
            best_qty = qty
            continue

        if is_bid:
            if px > best_price:
                best_price = px
                best_qty = qty
        else:
            if px < best_price:
                best_price = px
                best_qty = qty

    if best_price is None:
        return None
    return best_price, best_qty


def best_bid_ask(symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    Return (best_bid_px, best_ask_px) from WS-fed orderbook snapshot, or (None, None).
    """
    ob = get_orderbook_snapshot(symbol)
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []

    best_bid = _best_from_side(bids, is_bid=True)
    best_ask = _best_from_side(asks, is_bid=False)

    bid_px = best_bid[0] if best_bid else None
    ask_px = best_ask[0] if best_ask else None
    return bid_px, ask_px


def mid_price(symbol: str) -> Optional[Decimal]:
    """
    Return simple mid-price from best bid/ask, or None if missing.
    """
    bid, ask = best_bid_ask(symbol)
    if bid is None or ask is None:
        return None
    return (bid + ask) / Decimal("2")


def spread(symbol: str) -> Optional[Decimal]:
    """
    Return absolute spread (ask - bid) or None.
    """
    bid, ask = best_bid_ask(symbol)
    if bid is None or ask is None:
        return None
    return ask - bid


def spread_bps(symbol: str) -> Optional[Decimal]:
    """
    Spread in basis points (spread / mid * 10_000) or None.
    """
    m = mid_price(symbol)
    s = spread(symbol)
    if m is None or s is None or m <= 0:
        return None
    return (s / m) * Decimal("10000")


# ---------------------------------------------------------------------------
# Trades bus accessors
# ---------------------------------------------------------------------------

def _empty_trades_snapshot() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_ms": 0,
        "symbols": {},
    }


def load_trades_bus() -> Dict[str, Any]:
    """
    Return the entire trades_bus snapshot dict (never None).
    """
    snap = _load_json(TRADES_PATH)
    if snap is None:
        return _empty_trades_snapshot()

    if "symbols" not in snap or not isinstance(snap["symbols"], dict):
        snap["symbols"] = {}
    if "version" not in snap:
        snap["version"] = 1
    if "updated_ms" not in snap:
        snap["updated_ms"] = 0
    return snap


def trades_bus_updated_ms() -> int:
    """
    Return last updated_ms from trades_bus.json or 0.
    """
    snap = _load_json(TRADES_PATH)
    if not snap:
        return 0
    try:
        return int(snap.get("updated_ms", 0))
    except Exception:
        return 0


def get_recent_trades(symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Return up to `limit` most recent public trades for a symbol.

    Shape per trade depends on Bybit's publicTrade payload, typically:
      {
        "p": "price",
        "v": "size",
        "T": 1763751999000,   # timestamp ms
        "S": "Buy"/"Sell",
        ...
      }
    """
    sym = _normalize_symbol(symbol)
    snap = load_trades_bus()
    symbols = snap.get("symbols", {}) or {}
    entry = symbols.get(sym) or {}

    trades = entry.get("trades") or []
    if not isinstance(trades, list):
        return []

    if limit <= 0:
        return []
    if len(trades) <= limit:
        return trades
    return trades[-limit:]


def last_trade(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Return the most recent trade dict for symbol, or None.
    """
    trades = get_recent_trades(symbol, limit=1)
    if not trades:
        return None
    return trades[-1]
