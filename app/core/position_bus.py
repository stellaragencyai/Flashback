#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Position Bus (WS-friendly mirror over positions)

Purpose
-------
A thin abstraction layer that everyone else (TP/SL Manager, guards, etc.)
can query for "current positions" without caring whether the data comes
from:

  - A WS-fed snapshot file (written by ws_switchboard or another daemon), or
  - A direct REST call to Bybit (fallback).

Design
------
- Snapshot file:   state/positions_bus.json
- Structure:
    {
      "version": 1,
      "updated_ms": 1763752000123,
      "labels": {
        "main": {
          "category": "linear",
          "positions": [
            { ...Bybit position row... },
            ...
          ]
        },
        "flashback03": {
          "category": "linear",
          "positions": [...]
        }
      }
    }

- Callers typically use:
    from app.core.position_bus import (
        get_positions_for_label,
        get_position_map_for_label,
        get_positions_snapshot,
        get_positions_for_current_label,
        get_snapshot,
    )

    positions = get_positions_for_label(label="main", category="linear")
    pos_map   = get_position_map_for_label(label="main", category="linear")

Labels & accounts
-----------------
- Each WS writer (e.g. ws_switchboard) is responsible for writing a label block:
    labels["main"], labels["flashback10"], etc.
- Each process can identify "its" label via ACCOUNT_LABEL env var:

    ACCOUNT_LABEL=flashback10

Environment
-----------
POSITION_BUS_MAX_AGE_SECONDS   (default: "3")
POSITION_BUS_ALLOW_REST_WRITE  (default: "true")
ACCOUNT_LABEL                  (default: "main")

If POSITION_BUS_ALLOW_REST_WRITE=false :
    REST calls can read positions but will NOT overwrite positions_bus.json
    (useful once a WS writer owns the file).
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson

# Tolerant imports so tests / alt layouts still work
try:
    from app.core.config import settings
    from app.core.flashback_common import list_open_positions
except ImportError:  # pragma: no cover
    from core.config import settings  # type: ignore
    from core.flashback_common import list_open_positions  # type: ignore


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POS_SNAPSHOT_PATH: Path = STATE_DIR / "positions_bus.json"

# Max age (seconds) before snapshot is considered stale
_POSITION_BUS_MAX_AGE_SECONDS: int = int(os.getenv("POSITION_BUS_MAX_AGE_SECONDS", "3"))

# Whether REST fallback is allowed to *write* the snapshot
_POSITION_BUS_ALLOW_REST_WRITE: bool = os.getenv("POSITION_BUS_ALLOW_REST_WRITE", "true").strip().lower() in (
    "1",
    "true",
    "yes",
)

# Logical label for "this" account/process (main, flashback10, etc.)
ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_snapshot_raw() -> Optional[Dict[str, Any]]:
    """
    Load the entire snapshot dict from positions_bus.json, or None if missing/invalid.
    """
    try:
        if not POS_SNAPSHOT_PATH.exists():
            return None
        data = orjson.loads(POS_SNAPSHOT_PATH.read_bytes())
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


def _snapshot_age_seconds(snap: Dict[str, Any]) -> Optional[float]:
    """
    Return age in seconds if possible, else None.
    """
    try:
        updated_ms = int(snap.get("updated_ms"))
    except Exception:
        return None
    now_ms = _now_ms()
    if updated_ms <= 0 or now_ms <= updated_ms:
        return None
    return (now_ms - updated_ms) / 1000.0


def _save_snapshot(
    labels_positions: Dict[str, Dict[str, Any]],
) -> None:
    """
    Save a complete snapshot to disk.

    labels_positions format:
      {
        "main": {
          "category": "linear",
          "positions": [ {...}, {...}, ... ],
        },
        "flashback03": {
          "category": "linear",
          "positions": [ {...}, ... ],
        },
        ...
      }
    """
    snap = {
        "version": 1,
        "updated_ms": _now_ms(),
        "labels": labels_positions,
    }
    POS_SNAPSHOT_PATH.write_bytes(orjson.dumps(snap))


def get_snapshot() -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
    """
    Return (snapshot_dict, age_seconds).

    If the file is missing or invalid, returns (None, None).
    """
    snap = _load_snapshot_raw()
    if snap is None:
        return None, None
    age = _snapshot_age_seconds(snap)
    return snap, age


def _extract_label_positions(
    snap: Dict[str, Any],
    label: str,
    category: str,
) -> List[Dict[str, Any]]:
    """
    Given a snapshot, return the positions list for the given label+category.
    If label not present or category mismatched, returns [].
    """
    labels = snap.get("labels") or {}
    entry = labels.get(label)
    if not isinstance(entry, dict):
        return []
    entry_cat = str(entry.get("category", "")).lower()
    if entry_cat and entry_cat != category.lower():
        # Category mismatch: treat as none
        return []
    positions = entry.get("positions") or []
    if not isinstance(positions, list):
        return []
    return positions


def _rest_fetch_positions(category: str) -> List[Dict[str, Any]]:
    """
    Fallback to REST: use flashback_common.list_open_positions() and return its result.

    NOTE: list_open_positions() is already hard-wired for category='linear',
    so we do NOT pass a 'category' kwarg here.
    """
    try:
        rows = list_open_positions()
        if not isinstance(rows, list):
            return []
        return rows
    except Exception:
        return []


def _rest_refresh_snapshot_for_label(
    label: str,
    category: str,
    existing_snap: Optional[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Call REST to fetch positions for label+category (currently MAIN only),
    merge into a snapshot dict, and return (positions, updated_snapshot).

    For now, REST is used only for MAIN, since other labels typically map
    to subaccounts with their own auth context.
    """
    positions = _rest_fetch_positions(category=category)

    if existing_snap is None:
        labels_block: Dict[str, Dict[str, Any]] = {}
    else:
        labels_block = dict(existing_snap.get("labels") or {})

    labels_block[label] = {
        "category": category,
        "positions": positions,
    }

    new_snap = {
        "version": 1,
        "updated_ms": _now_ms(),
        "labels": labels_block,
    }
    return positions, new_snap


def get_positions_for_label(
    label: Optional[str] = "main",
    category: str = "linear",
    max_age_seconds: Optional[int] = None,
    allow_rest_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Main entry point.

    Returns a list of positions for a given label + category.

    Logic:
      1) Normalize label:
           - If label is None/empty, fall back to ACCOUNT_LABEL env.
      2) Try to read positions from positions_bus.json if:
           - file exists AND
           - age <= max_age_seconds (default: POSITION_BUS_MAX_AGE_SECONDS)
      3) If snapshot is missing/stale AND allow_rest_fallback:
           - use REST (list_open_positions) for MAIN only (label "main")
           - if POSITION_BUS_ALLOW_REST_WRITE, update positions_bus.json
      4) Otherwise, return [].

    Notes
    -----
    - For now, REST fallback is wired ONLY for label=="main".
      When WS-driven mirror is fully wired for subaccounts, they will
      appear in positions_bus.json and be read from there.
    """
    # 1) Normalize label first so callers passing label=None don't blow things up
    effective_label = label if isinstance(label, str) else None
    if not effective_label:
        effective_label = ACCOUNT_LABEL
    label = effective_label

    if max_age_seconds is None:
        max_age_seconds = _POSITION_BUS_MAX_AGE_SECONDS

    # 2) Try snapshot first
    snap, age = get_snapshot()
    if snap is not None and age is not None and age <= max_age_seconds:
        positions = _extract_label_positions(snap, label=label, category=category)
        if positions:
            return positions

    # 3) Snapshot missing or stale; optional REST fallback (MAIN only)
    if not allow_rest_fallback:
        return []

    if label.lower() != "main":
        # For subaccounts we rely on WS-fed snapshot; no REST fallback here by default.
        return []

    # REST fallback for MAIN
    positions, new_snap = _rest_refresh_snapshot_for_label(label="main", category=category, existing_snap=snap)

    # Optionally write snapshot to disk
    if _POSITION_BUS_ALLOW_REST_WRITE:
        try:
            POS_SNAPSHOT_PATH.write_bytes(orjson.dumps(new_snap))
        except Exception:
            # Do not let snapshot write failure kill callers
            pass

    return positions


def get_position_map_for_label(
    label: Optional[str] = "main",
    category: str = "linear",
    max_age_seconds: Optional[int] = None,
    allow_rest_fallback: bool = True,
    key_field: str = "symbol",
) -> Dict[str, Dict[str, Any]]:
    """
    Convenience wrapper: return a dict keyed by `key_field` (default "symbol").

    Example:
        pos_map = get_position_map_for_label(label="main", category="linear")
        btc_pos = pos_map.get("BTCUSDT")
    """
    rows = get_positions_for_label(
        label=label,
        category=category,
        max_age_seconds=max_age_seconds,
        allow_rest_fallback=allow_rest_fallback,
    )
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        try:
            key = str(row.get(key_field))
        except Exception:
            continue
        if not key:
            continue
        out[key] = row
    return out


# ---------------------------------------------------------------------------
# Env-label helpers (for current account / process)
# ---------------------------------------------------------------------------

def get_positions_for_current_label(
    category: str = "linear",
    max_age_seconds: Optional[int] = None,
    allow_rest_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Convenience: use ACCOUNT_LABEL as the label.

    Typical usage from bots scoped to one account:
        from app.core.position_bus import get_positions_for_current_label

        positions = get_positions_for_current_label()
    """
    return get_positions_for_label(
        label=ACCOUNT_LABEL,
        category=category,
        max_age_seconds=max_age_seconds,
        allow_rest_fallback=allow_rest_fallback,
    )


# ---------------------------------------------------------------------------
# Compatibility alias for TP/SL Manager v6.4+
# ---------------------------------------------------------------------------

def get_positions_snapshot(
    label: Optional[str] = None,
    category: str = "linear",
    max_age_seconds: Optional[int] = None,
    allow_rest_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    Simple alias used by tp_sl_manager and other modules.

    It just forwards to get_positions_for_label(), so older code that expects
    `get_positions_snapshot(label=..., category=...)` keeps working.

    If label is None, we default to ACCOUNT_LABEL (env).
    """
    effective_label = label or ACCOUNT_LABEL
    return get_positions_for_label(
        label=effective_label,
        category=category,
        max_age_seconds=max_age_seconds,
        allow_rest_fallback=allow_rest_fallback,
    )
