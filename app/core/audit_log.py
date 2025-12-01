#!/usr/bin/env python3
# app/core/audit_log.py
from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Dict, Any

import orjson

AUDIT_PATH = Path("state/audit_log.jsonl")
HASH_STATE_PATH = Path("state/audit_hash_state.json")

def _load_last_hash() -> str:
    try:
        data = orjson.loads(HASH_STATE_PATH.read_bytes())
        return data.get("last_hash", "")
    except Exception:
        return ""

def _save_last_hash(last_hash: str) -> None:
    HASH_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    HASH_STATE_PATH.write_bytes(orjson.dumps({"last_hash": last_hash}))

def log_event(event_type: str, data: Dict[str, Any]) -> None:
    """
    Append an immutable-like event:
      - ts
      - type
      - data
      - prev_hash
      - hash
    """
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    prev_hash = _load_last_hash()
    row = {
        "ts": int(time.time() * 1000),
        "type": event_type,
        "data": data,
        "prev_hash": prev_hash,
    }
    payload = orjson.dumps(row)
    h = hashlib.sha256(payload).hexdigest()
    row["hash"] = h

    with AUDIT_PATH.open("ab") as f:
        f.write(orjson.dumps(row) + b"\n")

    _save_last_hash(h)
