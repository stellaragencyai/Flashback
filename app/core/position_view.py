#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Positions View (WS-first, REST fallback)

Purpose
-------
Provide a simple, centralized way for bots (TP/SL manager, risk guardian, AI, etc.)
to read "current positions" per account:

    - Prefer WS-mirrored state from state_bus:
        state/positions_<label>.json
    - If empty / missing, fall back to REST:
        flashback_common.list_open_positions()

This keeps all the "where do positions come from?" logic in one place.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, Any

from app.core.state_bus import bus as state_bus
from app.core.flashback_common import list_open_positions


def _normalize_rest_position(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a REST position row into the same shape as WS-mirrored ones.
    This keeps downstream bots happy regardless of source.
    """
    sym = p.get("symbol")
    side = p.get("side")
    size = p.get("size")
    entry = p.get("avgPrice") or p.get("avgEntryPrice")
    liq = p.get("liqPrice") or p.get("liquidationPrice")

    return {
        "symbol": sym,
        "side": side,
        "size": size,
        "entryPrice": entry,
        "liquidationPrice": liq,
        "raw": p,
    }


def refresh_positions_from_rest(account_label: str) -> Dict[str, Dict[str, Any]]:
    """
    Fallback path: hit REST to get open positions and mirror them into state_bus.

    For now, we assume REST list_open_positions() returns MAIN unified positions.
    So:
      - If account_label.lower() != "main", we just return {} for fallback.
    """
    label = account_label.lower()
    if label != "main":
        # To support subs via REST later, you'd add per-sub REST calls here.
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    try:
        rows = list_open_positions()  # main unified account
    except Exception as e:
        print(f"[positions_view] REST list_open_positions error: {e}")
        return out

    for p in rows:
        try:
            sym = p.get("symbol")
            if not sym:
                continue
            norm = _normalize_rest_position(p)
            out[sym] = norm
            # Mirror into state bus for next time
            state_bus.set_position(label, sym, norm)
        except Exception as e:
            print(f"[positions_view] error normalizing position row: {e}")
            continue

    return out


def get_positions_for_label(account_label: str) -> Dict[str, Dict[str, Any]]:
    """
    Main entrypoint for bots.

    Usage:
        positions = get_positions_for_label("main")
        for symbol, pos in positions.items():
            ...

    Logic:
        1) Try WS-mirrored cache in state_bus.
        2) If empty, fall back to REST and re-populate the cache.
    """
    label = account_label.lower()
    cached = state_bus.all_positions(label)
    if cached:
        return cached

    # Cache is empty or missing, try REST
    return refresh_positions_from_rest(label)


def get_main_positions() -> Dict[str, Dict[str, Any]]:
    """
    Convenience wrapper for MAIN unified account.
    """
    return get_positions_for_label("main")
