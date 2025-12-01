#!/usr/bin/env python3
# app/core/vault.py
from __future__ import annotations

import time
from typing import Dict, Any

import orjson
from pathlib import Path

from app.core.audit_log import log_event

VAULT_STATE = Path("state/vault_locks.json")

def _load_state() -> Dict[str, Any]:
    try:
        return orjson.loads(VAULT_STATE.read_bytes())
    except Exception:
        return {}

def _save_state(st: Dict[str, Any]) -> None:
    VAULT_STATE.parent.mkdir(parents=True, exist_ok=True)
    VAULT_STATE.write_bytes(orjson.dumps(st))

def set_vault_lock(sub_label: str, unlock_ts: int, min_equity: float = 0.0) -> None:
    st = _load_state()
    st[sub_label] = {
        "unlock_ts": unlock_ts,
        "min_equity": min_equity,
    }
    _save_state(st)
    log_event("vault_lock_set", {"sub_label": sub_label, "unlock_ts": unlock_ts, "min_equity": min_equity})

def vault_allows(sub_label: str, current_equity: float) -> bool:
    st = _load_state()
    cfg = st.get(sub_label)
    if not cfg:
        return True
    now_ms = int(time.time() * 1000)
    if now_ms < int(cfg.get("unlock_ts", 0)):
        return False
    if current_equity < float(cfg.get("min_equity", 0.0)):
        return False
    return True
