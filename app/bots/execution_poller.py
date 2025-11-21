#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Execution Poller (HTTP fallback for WS Switchboard)

What this bot does
------------------
- Polls Bybit v5 REST for execution history on the MAIN account.
- Writes each NEW execution into the same bus used by ws_switchboard:

    state/ws_executions.jsonl

  Format per line:
    {
      "label": "main",
      "ts": <local_epoch_ms>,
      "row": { ... raw Bybit execution row ... }
    }

- Maintains a small state file so we only append NEW executions:
    state/execution_poller_state.json

This lets trade_journal v4.2 keep working even if WS subscriptions
are failing, by providing a HTTP-based fallback feed.

Notes
-----
- Category is hard-coded to "linear" (your perp setup).
- Uses /v5/execution/list with a rolling startTime filter.
- Only covers MAIN account (label="main") for now.
"""

from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson

from app.core.config import settings
from app.core.flashback_common import bybit_get
from app.core.logger import get_logger

log = get_logger("execution_poller")

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

EXEC_BUS_PATH = STATE_DIR / "ws_executions.jsonl"
STATE_PATH = STATE_DIR / "execution_poller_state.json"

CATEGORY = "linear"
POLL_SECONDS = 2.0  # how often to poll REST for new executions
PAGE_LIMIT = 200


def _load_state() -> Dict[str, Any]:
    """
    Load last seen execution cursor.

    We track:
      - last_exec_time_ms : int or 0
      - last_exec_id      : str or ""
    """
    try:
        data = orjson.loads(STATE_PATH.read_bytes())
        return {
            "last_exec_time_ms": int(data.get("last_exec_time_ms", 0) or 0),
            "last_exec_id": str(data.get("last_exec_id", "") or ""),
        }
    except Exception:
        return {"last_exec_time_ms": 0, "last_exec_id": ""}


def _save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_bytes(orjson.dumps(state))


def _append_exec(label: str, row: Dict[str, Any]) -> None:
    """
    Append a single execution row to ws_executions.jsonl in the same format
    that ws_switchboard_bot uses.
    """
    payload = {
        "label": label,
        "ts": int(time.time() * 1000),
        "row": row,
    }
    try:
        with EXEC_BUS_PATH.open("ab") as f:
            f.write(orjson.dumps(payload))
            f.write(b"\n")
    except Exception as e:
        log.warning("Failed to append execution to bus: %r", e)


def _fetch_executions_since(
    start_ms: Optional[int],
) -> List[Dict[str, Any]]:
    """
    Fetch executions via /v5/execution/list, optionally filtered by startTime.

    We only care about MAIN account, category=linear.
    """
    params: Dict[str, str] = {
        "category": CATEGORY,
        "limit": str(PAGE_LIMIT),
    }
    if start_ms and start_ms > 0:
        params["startTime"] = str(start_ms)

    try:
        r = bybit_get("/v5/execution/list", params)
    except Exception as e:
        log.warning("Execution API error: %r", e)
        return []

    result = r.get("result", {}) or {}
    rows = result.get("list", []) or []
    # Bybit tends to return newest first; we want oldest -> newest for clean state
    try:
        rows.sort(key=lambda x: int(str(x.get("execTime", "0"))))
    except Exception:
        pass
    return rows


def _is_new_exec(
    row: Dict[str, Any],
    last_time_ms: int,
    last_id: str,
) -> bool:
    """
    Decide if this execution is newer than our last-seen marker.
    """
    try:
        exec_time = int(str(row.get("execTime", "0")))
    except Exception:
        exec_time = 0
    exec_id = str(row.get("execId", "") or "")

    if exec_time > last_time_ms:
        return True
    if exec_time < last_time_ms:
        return False

    # Same timestamp: dedupe by execId lexicographically
    if not last_id:
        return True
    return exec_id > last_id


def loop() -> None:
    """
    Main polling loop. This is meant to be run under supervisor:

        python -m app.bots.execution_poller
    """
    log.info("Execution Poller starting (HTTP fallback).")
    log.info("Bus path: %s", EXEC_BUS_PATH)
    log.info("State path: %s", STATE_PATH)

    state = _load_state()
    last_time_ms = int(state.get("last_exec_time_ms", 0) or 0)
    last_id = str(state.get("last_exec_id", "") or "")

    log.info("Initial state: last_exec_time_ms=%s, last_exec_id=%s", last_time_ms, last_id or "<none>")

    while True:
        try:
            rows = _fetch_executions_since(last_time_ms)
            if not rows:
                time.sleep(POLL_SECONDS)
                continue

            new_seen = 0
            for row in rows:
                if not _is_new_exec(row, last_time_ms, last_id):
                    continue

                # Append to bus as MAIN
                _append_exec("main", row)
                new_seen += 1

                # Advance state marker
                try:
                    last_time_ms = int(str(row.get("execTime", last_time_ms)))
                except Exception:
                    pass
                last_id = str(row.get("execId", last_id) or last_id)

            if new_seen > 0:
                state["last_exec_time_ms"] = last_time_ms
                state["last_exec_id"] = last_id
                _save_state(state)
                log.info("Appended %d new executions; last_exec_time_ms=%s last_exec_id=%s",
                         new_seen, last_time_ms, last_id)

            time.sleep(POLL_SECONDS)
        except Exception as e:
            log.error("Execution poller loop error: %r", e)
            time.sleep(5.0)


if __name__ == "__main__":
    loop()
