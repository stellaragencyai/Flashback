#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Setup Memory (Stub v1)

Purpose:
    - Walk over all configured subaccounts (from strategies.yaml).
    - Create or update a simple model JSON file per sub_uid under MODEL_STORE_PATH.
    - For now, this only writes metadata + basic thresholds.
    - Later, this will:
        • Query the DB for each sub's historical trades.
        • Compute expectancy, RR distribution, feature stats.
        • Train actual models (classifiers, similarity, etc.).

Usage (for now):
    python -m app.ai.setup_memory   # refresh all sub models with defaults
"""

import os
from pathlib import Path
from typing import Any, Dict

# orjson preferred, fallback std json
try:
    import orjson as _orjson  # type: ignore

    def _dumps(obj: Any) -> bytes:
        return _orjson.dumps(obj)

except Exception:
    import json as _json

    def _dumps(obj: Any) -> bytes:
        return _json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

from app.core.strategies import all_sub_strategies

# ROOT = .../Flashback
ROOT = Path(__file__).resolve().parents[2]
MODEL_STORE_PATH = Path(
    os.getenv("MODEL_STORE_PATH", str(ROOT / "state" / "models"))
).resolve()

MODEL_STORE_PATH.mkdir(parents=True, exist_ok=True)


def _default_model_for_sub(sub_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build an initial model dict for a given subaccount strategy config.

    This is a placeholder: we encode some basic expectations so the
    AI gate has something to reference even before true training exists.
    """
    sub_uid = str(sub_cfg.get("sub_uid"))
    role = sub_cfg.get("role", "unknown")
    ai_profile = sub_cfg.get("ai_profile", "legacy_v1")

    # Simple, conservative defaults differ by role
    if role in ("trend_follow", "swing_trend", "correlation_filtered_trend"):
        min_trades = 100
        min_rr_mean = 0.1
        bad_hours = []   # trend systems usually OK most of the day
    elif role in ("breakout_impulse", "news_momentum", "pattern_breakout"):
        min_trades = 150
        min_rr_mean = 0.15
        bad_hours = [0, 1, 2, 3]  # avoid illiquid overnight hours initially
    elif role in ("mean_reversion", "canary_experimental"):
        min_trades = 80
        min_rr_mean = 0.05
        bad_hours = []
    else:
        min_trades = 50
        min_rr_mean = 0.0
        bad_hours = []

    return {
        "sub_uid": sub_uid,
        "role": role,
        "ai_profile": ai_profile,
        "min_trades": min_trades,
        "min_rr_mean": float(min_rr_mean),
        "bad_hours": bad_hours,
        "version": 1,
        "note": "stub model; thresholds will be replaced by trained stats later",
    }


def refresh_all_models() -> None:
    subs = all_sub_strategies()
    for cfg in subs:
        sub_uid = str(cfg.get("sub_uid"))
        if not sub_uid or sub_uid.lower() == "none":
            continue

        model = _default_model_for_sub(cfg)
        fname = MODEL_STORE_PATH / f"sub_{sub_uid}.json"
        fname.write_bytes(_dumps(model))


if __name__ == "__main__":
    print(f"Model store: {MODEL_STORE_PATH}")
    refresh_all_models()
    print("Refreshed stub models for all subaccounts.")
