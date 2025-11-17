#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Feature Store (trade-open snapshots for AI memory)

Purpose
-------
Central place to log feature snapshots whenever the executor opens a trade.

Each record is a single JSONL row with:
  - ts_ms, ts_iso
  - sub_uid, strategy, strategy_id (if any)
  - symbol, side, mode (PAPER/LIVE_CANARY/LIVE_FULL)
  - equity_usd, risk_usd, risk_pct
  - ai_score, ai_reason
  - signal metadata (reason, timeframe, raw signal)
  - feature vector used by the AI gate (flattened into JSON)

File:
  app/state/features_trades.jsonl  (append-only)
"""

from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional

import orjson

# Root = repo root (../../ from this file)
ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_TRADES = STATE_DIR / "features_trades.jsonl"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_iso(ts_ms: int) -> str:
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts_ms / 1000))
    except Exception:
        return str(ts_ms)


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, Decimal):
            return float(x)
        return float(x)
    except Exception:
        return None


def _json_serializable(obj: Any) -> Any:
    """
    Convert a few annoying types (Decimal) into JSON-friendly forms.
    """
    if isinstance(obj, Decimal):
        return str(obj)
    return obj


def log_trade_open(
    *,
    sub_uid: str,
    strategy: str,
    strategy_id: Optional[str],
    symbol: str,
    side: str,
    mode: str,
    equity_usd: Decimal,
    risk_usd: Decimal,
    risk_pct: Decimal,
    ai_score: float,
    ai_reason: str,
    features: Dict[str, Any],
    signal: Dict[str, Any],
) -> None:
    """
    Append a single trade-open feature snapshot to JSONL.

    Intended to be called from executor AFTER:
      - AI gate approves
      - Portfolio Guard approves
      - And right before sending the order.
    """
    ts_ms = _now_ms()
    row: Dict[str, Any] = {
        "ts_ms": ts_ms,
        "ts_iso": _to_iso(ts_ms),
        "source": "executor_v2",
        "account": "MAIN",

        "sub_uid": sub_uid,
        "strategy": strategy,
        "strategy_id": strategy_id,

        "symbol": symbol,
        "side": side,
        "mode": mode,

        "equity_usd": _to_float(equity_usd),
        "risk_usd": _to_float(risk_usd),
        "risk_pct": _to_float(risk_pct),

        "ai_score": _to_float(ai_score),
        "ai_reason": ai_reason,

        "signal_reason": signal.get("reason"),
        "signal_timeframe": signal.get("timeframe") or signal.get("tf"),
        "signal_raw": signal,   # full raw signal dict for later forensic / training
        "features": {k: _json_serializable(v) for k, v in features.items()},
    }

    try:
        with FEATURE_TRADES.open("ab") as f:
            f.write(orjson.dumps(row) + b"\n")
    except Exception:
        # Silent failure: feature logging must NEVER break trading.
        pass


# ----------------------------------------------------------------------
# Backward-compat shim for older code expecting `log_features(...)`
# ----------------------------------------------------------------------

def log_features(*, sub_uid: str, strategy: str, strategy_id: Optional[str],
                 symbol: str, side: str, mode: str,
                 equity_usd: Decimal, risk_usd: Decimal, risk_pct: Decimal,
                 ai_score: float, ai_reason: str,
                 features: Dict[str, Any], signal: Dict[str, Any]) -> None:
    """
    Back-compat wrapper so older executor code importing `log_features`
    still works. Internally delegates to `log_trade_open`.
    """
    return log_trade_open(
        sub_uid=sub_uid,
        strategy=strategy,
        strategy_id=strategy_id,
        symbol=symbol,
        side=side,
        mode=mode,
        equity_usd=equity_usd,
        risk_usd=risk_usd,
        risk_pct=risk_pct,
        ai_score=ai_score,
        ai_reason=ai_reason,
        features=features,
        signal=signal,
    )
