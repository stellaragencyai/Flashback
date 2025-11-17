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
    """
    Register / update a correlation estimate between two symbols.

    Args:
        sym_a: First symbol (e.g. "BTCUSDT", "BTCUSDT.PERP").
        sym_b: Second symbol.
        corr:  Correlation in [0, 1]. Values are clamped to this range.
    """
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
    """
    Get the static correlation between two symbols (normalized).

    Returns:
        1.0 if they are the same base asset, else the configured correlation,
        else 0.0 if unknown.
    """
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

    Args:
        symbol:    Candidate symbol to open.
        max_corr:  Minimum correlation threshold to count as "highly correlated".
        max_pairs: How many high-corr open mates you allow before blocking.

    Logic:
        - Normalize the candidate symbol.
        - Look at all open positions from list_open_positions().
        - For each open position with non-zero size:
            * compute corr(candidate, open_symbol)
            * if corr >= max_corr, count it as a "hit"
        - If hits > max_pairs → True (block), else False (allow).
    """
    open_pos: List[Dict[str, Any]] = list_open_positions()
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


def allow(
    symbol: str | None = None,
    *args: Any,
    **kwargs: Any,
) -> Tuple[bool, str]:
    """
    Corr-gate entry point used by executor_v2.

    This is intentionally flexible on signature so the executor can call it as:
        allow(symbol)
        allow(symbol=symbol)
        allow(symbol=symbol, max_corr=0.8, max_pairs=1, **meta)

    Args:
        symbol:    Candidate symbol to open (positional or kw).
        max_corr:  Optional override for correlation threshold (default 0.8).
        max_pairs: Optional override for max high-corr mates (default 1).

    Returns:
        (allowed: bool, reason: str)
    """
    # Allow both positional and keyword usage
    if symbol is None:
        symbol = kwargs.get("symbol")

    if not symbol:
        # Fail open but be explicit; better than crashing the executor.
        return True, "corr_gate_v2: no symbol provided, bypassing correlation check"

    max_corr = float(kwargs.get("max_corr", 0.8))
    max_pairs = int(kwargs.get("max_pairs", 1))

    if correlated_exposure_too_high(symbol, max_corr=max_corr, max_pairs=max_pairs):
        return (
            False,
            f"corr_gate_v2: blocked {symbol} — correlated exposure above "
            f"max_corr={max_corr}, max_pairs={max_pairs}",
        )

    return True, f"corr_gate_v2: OK for {symbol} (max_corr={max_corr}, max_pairs={max_pairs})"
