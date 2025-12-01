#!/usr/bin/env python3
# app/ai/setup_labels.py
from __future__ import annotations

from typing import Dict, Any

def infer_setup_label(trade: Dict[str, Any]) -> str:
    """
    Placeholder: classify trade into setup types based on its features.

    Later we will use real features (e.g. was it a breakout, pullback, reclaim).
    For now we rely on 'reason' from signal, if any.
    """
    reason = (trade.get("reason") or trade.get("signal_reason") or "").lower()
    if "breakout" in reason:
        return "breakout"
    if "pullback" in reason:
        return "pullback"
    if "reclaim" in reason or "deviation" in reason:
        return "reclaim"
    if "range" in reason or "mean" in reason:
        return "range"
    if "news" in reason:
        return "news"
    return "unknown"
