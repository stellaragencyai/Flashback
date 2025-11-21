#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Instruments Cache

Centralized caching for Bybit instrument metadata (tick size, lot size, etc.).
All bots should use this instead of hitting /v5/market/instruments over and over.
"""

import os
import time
from typing import Dict, Any, Optional, List

import requests
import orjson

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")

# Cache storage
_INSTRUMENTS_CACHE: Dict[str, Any] = {}
_LAST_FETCH_TS: float = 0.0
_TTL_SEC: int = 300  # 5 minutes by default


def _fetch_instruments(category: str = "linear") -> List[Dict[str, Any]]:
    url = f"{BYBIT_BASE}/v5/market/instruments-info"
    params = {"category": category}
    resp = requests.get(url, params=params, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Instruments fetch failed: {orjson.dumps(data).decode()}")
    return data.get("result", {}).get("list", [])


def _ensure_cache(category: str = "linear") -> None:
    global _LAST_FETCH_TS, _INSTRUMENTS_CACHE
    now = time.time()
    if now - _LAST_FETCH_TS < _TTL_SEC and _INSTRUMENTS_CACHE:
        return

    instruments = _fetch_instruments(category=category)
    cache: Dict[str, Any] = {}
    for inst in instruments:
        symbol = inst.get("symbol")
        if not symbol:
            continue
        cache[symbol] = inst

    _INSTRUMENTS_CACHE = cache
    _LAST_FETCH_TS = now


def get_instrument(symbol: str, category: str = "linear") -> Optional[Dict[str, Any]]:
    """
    Return raw instrument metadata for a symbol, or None if not found.
    """
    _ensure_cache(category=category)
    return _INSTRUMENTS_CACHE.get(symbol)


def get_precision(symbol: str, category: str = "linear") -> Dict[str, Any]:
    """
    Return a small dict with tickSize / lotSize, defaulting safely if unknown.
    """
    inst = get_instrument(symbol, category=category)
    if not inst:
        return {"tickSize": "0.0001", "lotSizeFilter": {"qtyStep": "0.001"}}

    return {
        "tickSize": inst.get("priceFilter", {}).get("tickSize", "0.0001"),
        "lotSizeFilter": inst.get("lotSizeFilter", {"qtyStep": "0.001"}),
    }
