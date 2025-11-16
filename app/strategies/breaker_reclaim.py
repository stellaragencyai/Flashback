#!/usr/bin/env python3
# app/strategies/breaker_reclaim.py
from __future__ import annotations

from decimal import Decimal
from typing import List, Dict, Any

from app.core.flashback_common import get_klines

def detect_breaker_reclaim(symbol: str, tf: str = "15", limit: int = 50) -> List[Dict[str, Any]]:
    """
    Very simplified:
      - detect local S/R as recent swing high/low
      - if price sweeps it and closes back inside, emit reclaim signal.
    """
    kl = get_klines(symbol, interval=tf, limit=limit)
    if len(kl) < 10:
        return []

    # identify a naive "key level" as previous close
    prev = kl[-2]
    prev_close = Decimal(str(prev[4]))

    last = kl[-1]
    high = Decimal(str(last[2]))
    low = Decimal(str(last[3]))
    close = Decimal(str(last[4]))

    signals: List[Dict[str, Any]] = []

    # Long reclaim: sweep below then close back above the level
    if low < prev_close and close > prev_close:
        signals.append({
            "symbol": symbol,
            "side": "Buy",
            "timeframe": f"{tf}m",
            "reason": "breaker_reclaim_long",
            "ts_ms": int(last[0]),
            "est_rr": 2.0,
        })

    # Short reclaim: sweep above then close back below the level
    if high > prev_close and close < prev_close:
        signals.append({
            "symbol": symbol,
            "side": "Sell",
            "timeframe": f"{tf}m",
            "reason": "breaker_reclaim_short",
            "ts_ms": int(last[0]),
            "est_rr": 2.0,
        })

    return signals
