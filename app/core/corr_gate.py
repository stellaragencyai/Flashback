#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Correlation Gate (v2 wrapper)

Purpose
-------
Prevent the executor from stacking too many positions in highly correlated
symbols at the same time.

Usage from executor:

    from app.core.corr_gate import allow as corr_allow

    if not corr_allow(symbol):
        # block trade
"""

from __future__ import annotations

# Try both app.core and core imports so it works in different launch modes
try:
    from app.core.corr_gate_v2 import (
        set_corr,
        get_corr,
        correlated_exposure_too_high,
    )
except ImportError:
    from core.corr_gate_v2 import (  # type: ignore
        set_corr,
        get_corr,
        correlated_exposure_too_high,
    )

__all__ = [
    "allow",
    "set_corr",
    "get_corr",
    "correlated_exposure_too_high",
]


def allow(symbol: str, max_corr: float = 0.8, max_pairs: int = 1) -> bool:
    """
    Return True if it's OK to open a new position in `symbol` given
    existing open positions.

    Internally uses `correlated_exposure_too_high` from corr_gate_v2.

    Args:
        symbol:     e.g. "BTCUSDT"
        max_corr:   correlation coefficient threshold to consider "highly correlated"
        max_pairs:  how many high-corr open mates you allow before blocking

    Returns:
        bool: True if *allowed*, False if we should block the trade.
    """
    too_high = correlated_exposure_too_high(
        symbol=symbol,
        max_corr=max_corr,
        max_pairs=max_pairs,
    )
    return not too_high
