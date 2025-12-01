#!/usr/bin/env python3
# app/ai/drift_watch.py
from __future__ import annotations

from decimal import Decimal
from typing import List, Dict, Any, Tuple

def compute_live_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    trades: recent live trades, merged from journal
    """
    if not trades:
        return {
            "count": 0,
            "winrate": 0.0,
            "avg_rr": 0.0,
        }

    wins = 0
    sum_rr = Decimal("0")
    cnt_rr = 0
    for t in trades:
        result = (t.get("result") or "").upper()
        if result == "WIN":
            wins += 1
        rr = t.get("realized_rr")
        if rr is not None:
            try:
                sum_rr += Decimal(str(rr))
                cnt_rr += 1
            except Exception:
                continue
    winrate = wins / len(trades)
    avg_rr = float(sum_rr / cnt_rr) if cnt_rr > 0 else 0.0
    return {
        "count": len(trades),
        "winrate": winrate,
        "avg_rr": avg_rr,
    }

def drift_detected(
    live_stats: Dict[str, Any],
    ref_stats: Dict[str, Any],
    winrate_tol: float = 0.15,
    rr_tol: float = 0.5,
) -> bool:
    """
    Return True if live performance is significantly worse than reference.
    """
    if live_stats["count"] < 30:
        return False  # too little data
    live_wr = live_stats.get("winrate", 0.0)
    live_rr = live_stats.get("avg_rr", 0.0)
    ref_wr = ref_stats.get("winrate", 0.5)
    ref_rr = ref_stats.get("avg_rr", 0.5)

    if live_wr < ref_wr - winrate_tol:
        return True
    if live_rr < ref_rr - rr_tol:
        return True
    return False
