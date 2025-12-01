#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 / Flashback — AI Store v1

Tiny file-based backend used by ai_hooks.

What it does:
- Provides three functions expected by app.core.ai_hooks:
    • log_signal(payload: dict)
    • log_order(payload: dict)
    • log_trade_summary(payload: dict)

- Each one appends a JSONL row to:
    state/ai/signals.jsonl
    state/ai/orders.jsonl
    state/ai/trades.jsonl

This is enough for:
- signal_engine
- executor
- journal
- future AI training scripts

Later we can swap this for a DB backend without changing the bots.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import os

# Try to respect your existing ROOT setting if present
try:
    from app.core.config import settings  # type: ignore
    ROOT = Path(getattr(settings, "ROOT", Path(__file__).resolve().parents[2]))
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state" / "ai"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SIGNAL_LOG = STATE_DIR / "signals.jsonl"
ORDER_LOG = STATE_DIR / "orders.jsonl"
TRADE_LOG = STATE_DIR / "trades.jsonl"

# Prefer orjson if installed, otherwise fallback to stdlib json
try:
    import orjson as _json  # type: ignore
    _USE_ORJSON = True
except Exception:
    import json as _json  # type: ignore
    _USE_ORJSON = False


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    """
    Append a single JSON-encoded row to the given file.
    Never raises; logs to stdout on error.
    """
    try:
        if _USE_ORJSON:
            data = _json.dumps(payload)
            with path.open("ab") as f:
                f.write(data + b"\n")
        else:
            line = _json.dumps(payload, ensure_ascii=False)
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        print(f"[ai_store] Failed to write {path.name}: {e}")


def log_signal(payload: Dict[str, Any]) -> None:
    """
    Persist a SignalLog payload from ai_hooks.log_signal_from_engine.
    """
    _append_jsonl(SIGNAL_LOG, payload)


def log_order(payload: Dict[str, Any]) -> None:
    """
    Persist an OrderLog payload from ai_hooks.log_order_basic.
    """
    _append_jsonl(ORDER_LOG, payload)


def log_trade_summary(payload: Dict[str, Any]) -> None:
    """
    Persist a TradeSummaryLog payload from ai_hooks.log_trade_summary_basic.
    """
    _append_jsonl(TRADE_LOG, payload)
