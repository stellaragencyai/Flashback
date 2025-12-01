#!/usr/bin/env python3
# app/strategies/siren.py
from __future__ import annotations

from decimal import Decimal
from typing import List, Dict, Any

from app.core.flashback_common import get_klines  # you likely already have this

def detect_siren_signals(symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Siren: detect sudden 1m ATR/volatility explosions and emit directional signals.

    For now we approximate via:
      - last 1m candle body vs average body
      - last volume vs avg volume
    """
    kl = get_klines(symbol, interval="1", limit=limit)
    if len(kl) < 20:
        return []
    # kl = [ [openTime, open, high, low, close, volume, ...], ... ]
    bodies: List[Decimal] = []
    vols: List[Decimal] = []
    for row in kl[:-1]:
        o = Decimal(str(row[1]))
        c = Decimal(str(row[4]))
        v = Decimal(str(row[5]))
        bodies.append(abs(c - o))
        vols.append(v)

    last = kl[-1]
    o_last = Decimal(str(last[1]))
    c_last = Decimal(str(last[4]))
    v_last = Decimal(str(last[5]))
    body_last = abs(c_last - o_last)

    avg_body = sum(bodies) / Decimal(len(bodies))
    avg_vol = sum(vols) / Decimal(len(vols))

    if avg_body <= 0 or avg_vol <= 0:
        return []

    body_factor = body_last / avg_body
    vol_factor = v_last / avg_vol

    # Conditions for a "siren" event
    if body_factor < Decimal("3") or vol_factor < Decimal("3"):
        return []

    side = "Buy" if c_last > o_last else "Sell"

    sig = {
        "symbol": symbol,
        "side": side,
        "timeframe": "1m",
        "reason": f"siren_vol_explosion_b{body_factor:.2f}_v{vol_factor:.2f}",
        "ts_ms": int(last[0]),
        "est_rr": 1.5,
    }
    return [sig]
