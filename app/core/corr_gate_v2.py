#!/usr/bin/env python3
# app/core/corr_gate_v2.py
from __future__ import annotations

from decimal import Decimal
from typing import Dict, Any, List, Tuple

from app.core.flashback_common import list_open_positions

# Simple static correlation map. Adjust over time.
# Values: 0.0–1.0 (1.0 = highly correlated).
_CORR: Dict[Tuple[str, str], float] = {}


def _norm(sym: str) -> str:
    return sym.upper().replace("PERP", "").replace("USDT", "")


def set_corr(sym_a: str, sym_b: str, corr: float) -> None:
    a = _norm(sym_a)
    b = _norm(sym_b)
    if a == b:
        return
    if corr < 0:
        corr = 0.0
    if corr > 1:
        corr = 1.0
    key1 = (a, b)
    key2 = (b, a)
    _CORR[key1] = corr
    _CORR[key2] = corr


def get_corr(sym_a: str, sym_b: str) -> float:
    a = _norm(sym_a)
    b = _norm(sym_b)
    if a == b:
        return 1.0
    return _CORR.get((a, b), 0.0)


def correlated_exposure_too_high(
    symbol: str,
    max_corr: float = 0.8,
    max_pairs: int = 1,
) -> bool:
    """
    Return True if opening a new position in `symbol` would create
    too much correlated exposure with existing open positions.

    max_pairs: how many high-corr open mates you allow before blocking.
    """
    open_pos = list_open_positions()
    if not open_pos:
        return False

    base = _norm(symbol)
    hits = 0
    for p in open_pos:
        sym = p.get("symbol") or ""
        size = Decimal(str(p.get("size", "0")))
        if size <= 0:
            continue
        corr = get_corr(base, sym)
        if corr >= max_corr:
            hits += 1
            if hits > max_pairs:
                return True
    return False
