#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Feature Snapshot Logger v2

Logs features at *trade OPEN* time into state/features_trades.jsonl.

Each row:
  {
    "ts_open_ms": ...,
    "symbol": "...",
    "sub_uid": "...",
    "strategy_name": "...",
    "setup_type": "...",
    "mode": "...",
    "features": {
        "adx_14_1h": ...,
        "atr_pct_1h": ...,
        "vol_z_5m": ...,
        "spread_bps": ...,
        "depth_imbalance": ...,
        "trend_state": "...",
        ...
    }
  }
"""

from pathlib import Path
from typing import Dict, Any

import orjson

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
FEATURE_PATH = STATE_DIR / "features_trades.jsonl"


def log_features_at_open(
    *,
    ts_open_ms: int,
    symbol: str,
    sub_uid: str,
    strategy_name: str,
    setup_type: str,
    mode: str,
    features: Dict[str, Any],
) -> None:
    row: Dict[str, Any] = {
        "ts_open_ms": int(ts_open_ms),
        "symbol": symbol.upper(),
        "sub_uid": str(sub_uid),
        "strategy_name": strategy_name,
        "setup_type": setup_type,
        "mode": mode,
        "features": features,
    }
    with FEATURE_PATH.open("ab") as f:
        f.write(orjson.dumps(row) + b"\n")
