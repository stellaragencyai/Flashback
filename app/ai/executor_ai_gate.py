#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Executor Gate

Single interface between the Auto Executor and whatever AI/logic
decides whether a given setup is worth trading.

Current version:
    - Loads a per-sub "model" JSON from MODEL_STORE_PATH / sub_<uid>.json
    - If none exists, falls back to a simple default policy:
        -> allow = True, score = 1.0, reason = "no model; default allow"
    - The "model" format is intentionally simple so we can start with
      handcrafted thresholds and later replace it with real ML output.

Expected model JSON structure (example):
    {
        "sub_uid": "524630315",
        "ai_profile": "trend_v1",
        "min_trades": 100,
        "min_rr_mean": 0.1,
        "bad_hours": [0, 1, 2],           # optional
        "tags": ["trend_follow", "early"]
    }

The gate will gradually evolve to:
    - Use feature hashes and similarity scores
    - Consult per-setup winrate / expectancy
    - Enforce guardrails when models are uncertain or drifted
"""

import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Tuple

# orjson preferred, fallback to std json
try:
    import orjson as _orjson  # type: ignore

    def _loads(b: bytes) -> Any:
        return _orjson.loads(b)

except Exception:
    import json as _json

    def _loads(b: bytes) -> Any:
        if isinstance(b, (bytes, bytearray)):
            b = b.decode("utf-8")
        return _json.loads(b)

# ROOT = .../Flashback
ROOT = Path(__file__).resolve().parents[2]

MODEL_STORE_PATH = Path(
    os.getenv("MODEL_STORE_PATH", str(ROOT / "state" / "models"))
).resolve()

MODEL_STORE_PATH.mkdir(parents=True, exist_ok=True)


def _load_model_for_sub(sub_uid: str) -> Dict[str, Any]:
    """
    Load model JSON for a given sub_uid, or return an empty dict if missing.
    """
    fname = MODEL_STORE_PATH / f"sub_{sub_uid}.json"
    if not fname.exists():
        return {}
    try:
        data = _loads(fname.read_bytes())
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def ai_gate_decide(sub_uid: str,
                   strategy_id: str,
                   features: Dict[str, Any]) -> Tuple[bool, float, str]:
    """
    Main entrypoint for the executor.

    Arguments:
        sub_uid:    Which subaccount is trying to trade.
        strategy_id:Typically same as role or ai_profile (e.g., "trend_v1").
        features:   Dict of numerical / categorical features for the setup
                    (rr_estimate, adx, atr_pct, vol_zscore, time_of_day, etc.)

    Returns:
        (allowed: bool, score: float [0..1], reason: str)
    """
    sub_uid_str = str(sub_uid)
    model = _load_model_for_sub(sub_uid_str)

    if not model:
        # No model yet: default allow, but be explicit in the reason.
        return True, 1.0, "no_model_default_allow"

    # Example simple policy:
    #   - if model has min_trades and we haven't reached it (from features), be cautious
    #   - if model has min_rr_mean and estimated_rr < min_rr_mean, reject

    min_trades = int(model.get("min_trades", 0))
    min_rr_mean = Decimal(str(model.get("min_rr_mean", "0")))
    bad_hours = model.get("bad_hours", [])

    # Features we expect (optional; missing values are tolerated)
    trade_count = int(features.get("sub_trade_count", 0))
    est_rr = Decimal(str(features.get("est_rr", "0")))
    hour = int(features.get("hour_of_day", -1))

    # Too few trades to trust the model -> allow but with low score
    if trade_count < min_trades and min_trades > 0:
        return True, 0.6, f"below_min_trades:{trade_count}/{min_trades}"

    # Time-of-day filter
    if bad_hours and hour in bad_hours:
        return False, 0.1, f"blocked_bad_hour:{hour}"

    # RR filter
    if est_rr < min_rr_mean:
        return False, float(max(0.0, float(est_rr))), f"est_rr_below_min:{est_rr}<{min_rr_mean}"

    # Otherwise: allowed, with decent score
    return True, 0.9, "passes_simple_policy"


if __name__ == "__main__":
    # Tiny smoke test
    ok, score, reason = ai_gate_decide(
        sub_uid="TEST",
        strategy_id="trend_v1",
        features={"sub_trade_count": 50, "est_rr": 0.2, "hour_of_day": 10},
    )
    print("AI gate decision:", ok, score, reason)
