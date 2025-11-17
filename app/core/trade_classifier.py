#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Trade classifier (stub)

Executor expects a `classify(...)` function returning:
    {"allow": bool, "reason": str, ...}

This stub always allows trades. Upgrade later with real rules/ML.
"""

from __future__ import annotations

from typing import Dict, Any, Optional


def classify(
    symbol: str,
    timeframe: str,
    side: str,
    price: float,
    ts: Optional[int] = None,
) -> Dict[str, Any]:
    return {
        "allow": True,
        "reason": "stub-allow",
        "symbol": symbol,
        "timeframe": timeframe,
        "side": side,
        "price": price,
        "ts": ts,
    }
