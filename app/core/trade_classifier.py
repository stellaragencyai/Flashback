#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Trade Classifier v2.0

Purpose
-------
Unified classifier used by the executor + setup memory.

Two modes:

1) NEW AI GATE MODE (used by executor_v2):
    classify(signal: dict, strat_id: str) -> dict

    Returns:
      {
        "allow": bool,        # model-based yes/no (basic)
        "score": float | None,# probability from model [0,1] if available
        "reason": str,        # human-readable summary
        "features": dict      # feature dict used for scoring & logging
      }

    - Uses trained model from models/setup_classifier.pkl if present.
    - Falls back gracefully if model/meta missing or broken.

2) LEGACY LABEL MODE (backward compat):
    classify(signal: dict, features: dict) -> str

    - Deterministic heuristic label:
        "breakout_trend", "pullback_trend", "range_fade",
        "vol_squeeze_break", "news_spike", "trend_momentum", "unknown"
    - The old behavior is preserved via an internal helper.

Executor usage (new):
    from app.core.trade_classifier import classify as classify_trade
    clf = classify_trade(signal, strat_id)

Other legacy usage (if any) still works:
    label = classify(signal, features_dict)
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    from app.core.logger import get_logger
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
        logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


log = get_logger("trade_classifier")

# ROOT / models path
try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = settings.ROOT
MODELS_DIR: Path = ROOT / "models"
MODEL_PATH: Path = MODELS_DIR / "setup_classifier.pkl"
META_PATH: Path = MODELS_DIR / "setup_classifier_meta.json"


# ---------------------------------------------------------------------------
# Legacy heuristic labeler (kept as-is)
# ---------------------------------------------------------------------------

def _get_lower(d: Dict[str, Any], key: str) -> str:
    v = d.get(key)
    if v is None:
        return ""
    return str(v).strip().lower()


def _legacy_label(signal: Dict[str, Any], features: Dict[str, Any]) -> str:
    """
    Legacy deterministic tagger:
        (signal, features) -> short label string
    """

    reason = _get_lower(signal, "reason")
    pattern = _get_lower(signal, "pattern")
    regime = _get_lower(features, "regime")
    structure = _get_lower(features, "structure") or _get_lower(features, "market_structure")

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
        if any(k in reason for k in ("fade", "revert", "mean")):
            return "range_fade"
        return "range_play"

    # 3) Volatility squeeze then pop
    if "squeeze" in reason or "squeeze" in pattern:
        if vol_z > 1.5 or atr_pct > 1.0:
            return "vol_squeeze_break"
        return "vol_squeeze"

    # 4) News-ish spikes
    if any(k in reason for k in ("news", "event", "fomc", "earnings")):
        return "news_spike"

    # 5) Default momentum in trend
    if adx >= 20:
        return "trend_momentum"

    return "unknown"


# ---------------------------------------------------------------------------
# Model loader / live feature extraction
# ---------------------------------------------------------------------------

_MODEL = None
_MODEL_FEATURES: Optional[list[str]] = None
_MODEL_META: Dict[str, Any] = {}


def _load_model_once() -> None:
    global _MODEL, _MODEL_FEATURES, _MODEL_META

    if _MODEL is not None:
        return

    if not MODEL_PATH.exists() or not META_PATH.exists():
        log.info("No trained model artifacts found; classifier will run in fallback mode.")
        _MODEL = None
        _MODEL_FEATURES = None
        _MODEL_META = {}
        return

    try:
        import joblib  # type: ignore
    except Exception as e:
        log.warning("joblib not available to load model: %r", e)
        _MODEL = None
        _MODEL_FEATURES = None
        _MODEL_META = {}
        return

    try:
        _MODEL = joblib.load(MODEL_PATH)
        meta_raw = META_PATH.read_text()
        _MODEL_META = json.loads(meta_raw)
        _MODEL_FEATURES = _MODEL_META.get("feature_names") or []
        log.info("Loaded setup classifier model from %s", MODEL_PATH)
    except Exception as e:
        log.exception("Failed to load model/meta: %r", e)
        _MODEL = None
        _MODEL_FEATURES = None
        _MODEL_META = {}


def _session_to_int(session: str) -> int:
    s = (session or "").upper()
    if s == "ASIA":
        return 0
    if s == "LONDON":
        return 1
    if s == "NEW_YORK":
        return 2
    if s == "POST":
        return 3
    return -1


def _derive_session_from_ts(ts_ms: Optional[int]) -> str:
    """
    Rough session from timestamp (UTC). If ts is missing, use current time.
    """
    import datetime as dt

    if ts_ms is None:
        now = dt.datetime.utcnow()
    else:
        now = dt.datetime.utcfromtimestamp(ts_ms / 1000.0)

    hour = now.hour
    if 0 <= hour < 7:
        return "ASIA"
    if 7 <= hour < 13:
        return "LONDON"
    if 13 <= hour < 20:
        return "NEW_YORK"
    return "POST"


def _extract_live_features(signal: Dict[str, Any]) -> Tuple[Dict[str, Any], list[float]]:
    """
    Build a feature dict + vector for the live signal, aligned (as much as possible)
    with the features used in training.

    Training feature names (from train_models.py):
        [
          "side_sign",
          "atr_like",
          "atr_pct",
          "range_mean",
          "range_std",
          "volume_zscore",
          "trend_dir",
          "trend_strength",
          "entry_hour",
          "entry_dow",
          "session_int"
        ]
    """
    import datetime as dt

    side_raw = str(signal.get("side") or "").lower()
    if side_raw in ("buy", "long"):
        side_sign = 1
    elif side_raw in ("sell", "short"):
        side_sign = -1
    else:
        side_sign = 0

    # Numeric fields with graceful fallback
    def _f(keys, default=0.0) -> float:
        for k in keys:
            if k in signal and signal[k] is not None:
                try:
                    return float(signal[k])
                except Exception:
                    continue
        return float(default)

    atr_like = _f(["atr_like"], 0.0)
    atr_pct = _f(["atr_pct", "atr_percent"], 0.0)
    range_mean = _f(["range_mean"], 0.0)
    range_std = _f(["range_std"], 0.0)

    # Volume zscore might be named in different ways
    volume_zscore = _f(["volume_zscore", "vol_z", "volume_z"], 0.0)

    # Trend dir/strength could be precomputed; if not, we leave neutral
    trend_dir = _f(["trend_dir"], 0.0)
    trend_strength = _f(["trend_strength"], 0.0)

    ts = signal.get("ts") or signal.get("timestamp")
    try:
        ts_ms = int(ts)
    except Exception:
        ts_ms = None

    if ts_ms is not None:
        dt_obj = dt.datetime.utcfromtimestamp(ts_ms / 1000.0)
    else:
        dt_obj = dt.datetime.utcnow()

    entry_hour = dt_obj.hour
    entry_dow = dt_obj.weekday()

    session = _derive_session_from_ts(ts_ms)
    session_int = _session_to_int(session)

    feature_dict: Dict[str, Any] = {
        "side_sign": side_sign,
        "atr_like": atr_like,
        "atr_pct": atr_pct,
        "range_mean": range_mean,
        "range_std": range_std,
        "volume_zscore": volume_zscore,
        "trend_dir": trend_dir,
        "trend_strength": trend_strength,
        "entry_hour": entry_hour,
        "entry_dow": entry_dow,
        "session": session,
        "session_int": session_int,
    }

    # Default feature order used by training
    default_names = [
        "side_sign",
        "atr_like",
        "atr_pct",
        "range_mean",
        "range_std",
        "volume_zscore",
        "trend_dir",
        "trend_strength",
        "entry_hour",
        "entry_dow",
        "session_int",
    ]

    names = _MODEL_FEATURES if _MODEL_FEATURES else default_names
    vec = [float(feature_dict.get(name, 0.0)) for name in names]

    return feature_dict, vec


# ---------------------------------------------------------------------------
# AI classifier entry (used by executor_v2)
# ---------------------------------------------------------------------------

def _classify_ai(signal: Dict[str, Any], strat_id: str) -> Dict[str, Any]:
    """
    New AI gate interface:
      (signal, strat_id) -> dict with allow, score, reason, features
    """

    _load_model_once()

    # Extract live features for logging & potential scoring
    features_dict, vec = _extract_live_features(signal)

    # Attach legacy label as an extra feature (for setup memory)
    try:
        leg_label = _legacy_label(signal, features_dict)
    except Exception:
        leg_label = "unknown"
    features_dict["setup_label_heuristic"] = leg_label

    if _MODEL is None:
        # No model: allow by default, no score, but still return features
        return {
            "allow": True,
            "score": None,
            "reason": "no_model_loaded",
            "features": features_dict,
        }

    # Try model inference
    try:
        # scikit-learn pipeline expects 2D array
        probs = _MODEL.predict_proba([vec])[0]  # type: ignore[attr-defined]
        score = float(probs[1]) if len(probs) > 1 else float(probs[0])
    except Exception as e:
        log.warning("Model inference failed for %s: %r", strat_id, e)
        return {
            "allow": True,
            "score": None,
            "reason": f"model_inference_error: {e}",
            "features": features_dict,
        }

    # Basic allow rule: score >= 0.5
    allow = bool(score >= 0.5)

    return {
        "allow": allow,
        "score": score,
        "reason": "ok",
        "features": features_dict,
    }


# ---------------------------------------------------------------------------
# Public entrypoint with dual behavior
# ---------------------------------------------------------------------------

def classify(*args, **kwargs):
    """
    Public entry with dual behavior:

    1) NEW AI MODE (executor_v2):
        classify(signal: dict, strat_id: str) -> dict

    2) LEGACY LABEL MODE:
        classify(signal: dict, features: dict) -> str

    We branch based on types of the second arg.
    """
    # Keyword-based dispatch (just in case)
    if "strat_id" in kwargs or "strategy_id" in kwargs:
        signal = kwargs.get("signal") or (args[0] if args else {})
        strat_id = kwargs.get("strat_id") or kwargs.get("strategy_id")
        return _classify_ai(signal, str(strat_id))

    # Positional dispatch
    if len(args) == 2:
        a0, a1 = args
        # New AI usage: (signal, strat_id) where strat_id is a string
        if isinstance(a1, str):
            return _classify_ai(a0, a1)
        # Legacy usage: (signal, features_dict)
        if isinstance(a1, dict):
            return _legacy_label(a0, a1)

    # Fallback: try to guess
    if len(args) == 1 and isinstance(args[0], dict):
        # Just signal? not great; treat as "unknown"
        return "unknown"

    raise TypeError(
        "classify() expected (signal, strat_id:str) or (signal, features:dict); "
        f"got args={args}, kwargs={kwargs}"
    )
