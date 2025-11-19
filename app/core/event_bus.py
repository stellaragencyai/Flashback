#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Event Bus Core (JSONL-based)

Purpose
-------
Provide a tiny, shared helper around the normalized event bus:

    state/event_bus.jsonl

Each line in the bus is a single JSON object, typically in the form:

    {
      "type": "EXECUTION" | "POSITION" | ...,
      "label": "main" | "flashback01" | ...,
      "ts": 1731870000000,
      ... other fields ...,
      "raw": { ... original WS row ... }
    }

This module **does not** interpret events.
It only:
  - appends events (if you want)
  - lets consumers read new events from a given byte offset
  - handles per-consumer cursors in state/

Bots use it like:

    from app.core import event_bus

    CURSOR_PATH = STATE_DIR / "journal.event_cursor"

    events, new_pos = event_bus.read_events(
        start_pos=event_bus.load_cursor(CURSOR_PATH),
        allowed_types={"EXECUTION"},
        allowed_labels={"main"},
    )
    # process events...
    event_bus.save_cursor(CURSOR_PATH, new_pos)
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, Any, Iterable, Optional, Set, Tuple, List

import orjson

try:
    from app.core.config import settings
except ImportError:  # pragma: no cover
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

EVENT_BUS_PATH: Path = STATE_DIR / "event_bus.jsonl"


# ---------- cursor helpers ----------

def load_cursor(cursor_path: Path) -> int:
    """
    Load a consumer's byte offset cursor.

    Returns 0 if missing or invalid.
    """
    try:
        if not cursor_path.exists():
            return 0
        data = orjson.loads(cursor_path.read_bytes())
        pos = int(data.get("pos", 0))
        return max(0, pos)
    except Exception:
        return 0


def save_cursor(cursor_path: Path, pos: int) -> None:
    """
    Save a consumer's byte offset cursor.
    """
    try:
        cursor_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"pos": int(pos), "ts": int(time.time() * 1000)}
        cursor_path.write_bytes(orjson.dumps(payload))
    except Exception:
        # Cursor failure should not kill the bot that uses it.
        pass


# ---------- event read / append ----------

def append_event(event: Dict[str, Any]) -> None:
    """
    Append a single event to the global event bus.

    Normally the WS switchboard is the main producer, but other
    components can push synthetic events as well:

        append_event({
            "type": "GUARD_TRIP",
            "label": "main",
            "reason": "DAILY_DD_LIMIT",
            "dd_pct": -3.2,
        })

    If 'ts' is absent, it is auto-filled with current epoch ms.
    """
    try:
        if "ts" not in event:
            event["ts"] = int(time.time() * 1000)
        EVENT_BUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with EVENT_BUS_PATH.open("ab") as f:
            f.write(orjson.dumps(event))
            f.write(b"\n")
    except Exception:
        # Appends are best-effort; do not throw.
        pass


def read_events(
    start_pos: int,
    allowed_types: Optional[Iterable[str]] = None,
    allowed_labels: Optional[Iterable[str]] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Read new events from the bus starting at byte offset `start_pos`.

    Returns (events, new_pos) where:
      - events   : list of event dicts matching filters
      - new_pos  : new byte offset after reading

    Filters:
      - allowed_types  : iterable of event types (e.g. {"EXECUTION", "POSITION"})
                         If None or empty, all types are accepted.
      - allowed_labels : iterable of account labels (e.g. {"main", "flashback02"})
                         If None or empty, all labels are accepted.
    """
    events: List[Dict[str, Any]] = []
    pos = start_pos

    if not EVENT_BUS_PATH.exists():
        return events, pos

    try:
        file_size = EVENT_BUS_PATH.stat().st_size
    except Exception:
        return events, pos

    # Handle rotation / truncation
    if start_pos > file_size:
        start_pos = 0

    type_filter: Optional[Set[str]] = None
    label_filter: Optional[Set[str]] = None

    if allowed_types:
        type_filter = {t.upper() for t in allowed_types}
    if allowed_labels:
        label_filter = set(allowed_labels)

    try:
        with EVENT_BUS_PATH.open("rb") as f:
            f.seek(start_pos)
            for line in f:
                pos = f.tell()
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = orjson.loads(line)
                except Exception:
                    continue

                ev_type = str(ev.get("type", "")).upper()
                ev_label = ev.get("label")

                if type_filter is not None and ev_type not in type_filter:
                    continue
                if label_filter is not None and ev_label not in label_filter:
                    continue

                events.append(ev)
    except Exception:
        # On read failure, just return whatever we collected and the last pos.
        return events, pos

    return events, pos
