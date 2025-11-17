#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Trade Classifier (v1, heuristic)

Purpose
-------
Tiny helper used by the executor + feature store to tag each trade with a
high-level "setup class" for later analysis / training.

This is *not* a ML model yet; it's a deterministic rule-based tagger that
looks at the signal + features and returns a short label:

Examples:
    "breakout_trend"
    "pullback_trend"
    "range_fade"
    "news_spike"
    "vol_squeeze_break"
    "unknown"

Executor usage:
    from app.core.trade_classifier import classify as classify_trade

    setup_tag = classify_trade(signal=signal_dict, features=feature_dict)
"""

from __future__ import annotations

from typing import Dict, Any


def _get_lower(d: Dict[str, Any], key: str) -> str:
    v = d.get(key)
    if v is None:
        return ""
    return str(v).strip().lower()


def classify(signal: Dict[str, Any], features: Dict[str, Any]) -> str:
    """
    Return a short string label for the trade setup.

    Inputs:
        signal  - raw signal dict passed from signal engine / executor
        features - feature vector dict (adx, atr_pct, vol_z, mkt_structure, etc.)

    Returns:
        label: str
    """
    # Basic fields
    reason = _get_lower(signal, "reason")
    pattern = _get_lower(signal, "pattern")
    regime = _get_lower(features, "regime")
    structure = _get_lower(features, "structure") or _get_lower(features, "market_structure")

    # Numeric helpers
    try:
        adx = float(features.get("adx", 0.0))
    except Exception:
        adx = 0.0
    try:
        atr_pct = float(features.get("atr_pct", 0.0))
    except Exception:
        atr_pct = 0.0
    try:
        vol_z = float(features.get("vol_z", 0.0))
    except Exception:
        vol_z = 0.0

    # 1) Strong trend + breakout-ish reason
    if adx >= 20 and ("breakout" in reason or "breakout" in pattern):
        if "pullback" in reason or "retest" in reason:
            return "pullback_trend"
        return "breakout_trend"

    # 2) Clear range structure
    if "range" in structure or "range" in reason:
        if "fade" in reason or "revert" in reason or "mean" in reason:
            return "range_fade"
        return "range_play"

    # 3) Volatility squeeze then pop
    if "squeeze" in reason or "squeeze" in pattern:
        if vol_z > 1.5 or atr_pct > 1.0:
            return "vol_squeeze_break"
        return "vol_squeeze"

    # 4) News-ish spikes
    if "news" in reason or "event" in reason or "fomc" in reason or "earnings" in reason:
        return "news_spike"

    # 5) Default: unknown / generic momentum
    if adx >= 20:
        return "trend_momentum"

    return "unknown"
