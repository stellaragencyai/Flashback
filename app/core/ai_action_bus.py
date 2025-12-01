#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Bus

Purpose
-------
Central log for AI "actions" (decisions / recommendations) emitted by
ai_pilot or future AI agents.

File format
-----------
JSON Lines (one object per line), so it's easy to tail/grep or load into
pandas.

Location
--------
- Controlled by AI_ACTIONS_PATH env var
- Defaults to: "state/ai_actions.jsonl" under ROOT

Each line looks like:

    {
      "ts_ms": 1763752000123,
      "source": "ai_pilot",
      "label": "main",
      "dry_run": true,
      "action": {
        "type": "advice_only",
        "reason": "sample_policy",
        "symbol": "BTCUSDT",
        "side": "Buy",
        "size": "0.005",
        ...
      }
    }

Nothing here places orders. This is a log / bus only.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import orjson

# ---------------------------------------------------------------------------
# ROOT & path resolution
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
except Exception:  # pragma: no cover
    class _DummySettings:  # type: ignore
        ROOT: Path = Path(__file__).resolve().parents[2]

    settings = _DummySettings()  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])

_DEFAULT_PATH = "state/ai_actions.jsonl"
_RAW_ACTIONS_PATH = os.getenv("AI_ACTIONS_PATH", _DEFAULT_PATH)

ACTION_LOG_PATH: Path = Path(_RAW_ACTIONS_PATH)
if not ACTION_LOG_PATH.is_absolute():
    ACTION_LOG_PATH = ROOT / ACTION_LOG_PATH

ACTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def append_actions(
    actions: Iterable[Dict[str, Any]],
    *,
    source: str = "ai_pilot",
    label: Optional[str] = None,
    dry_run: Optional[bool] = None,
) -> int:
    """
    Append a batch of actions to the AI action log.

    Parameters
    ----------
    actions : iterable of dict
        Raw action dicts produced by AI policies.
    source : str
        Logical producer ("ai_pilot", "ai_guard", "ai_risk", etc.).
    label : str, optional
        Account label (e.g. "main", "flashback10"). If omitted, any "label"
        inside each action is used, falling back to "unknown".
    dry_run : bool, optional
        If provided, forces the "dry_run" flag in the envelope; otherwise
        uses action.get("dry_run", True).

    Returns
    -------
    int
        Number of actions successfully written.
    """
    buf: List[bytes] = []
    ts = _now_ms()

    for a in actions:
        if not isinstance(a, dict):
            continue

        env_label = label or str(a.get("label", "") or "unknown")
        env_dry = dry_run if dry_run is not None else bool(a.get("dry_run", True))

        row = {
            "ts_ms": ts,
            "source": source,
            "label": env_label,
            "dry_run": env_dry,
            "action": a,
        }

        try:
            buf.append(orjson.dumps(row) + b"\n")
        except Exception:
            # Skip bad rows rather than nuking the caller.
            continue

    if not buf:
        return 0

    try:
        with ACTION_LOG_PATH.open("ab") as f:
            for b in buf:
                f.write(b)
    except Exception:
        # Log failure is non-fatal. Caller can decide how much they care.
        return 0

    return len(buf)
