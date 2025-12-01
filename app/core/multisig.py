#!/usr/bin/env python3
# app/core/multisig.py
from __future__ import annotations

import os
import time
import secrets
from typing import Dict, Any

import orjson
from pathlib import Path

from app.core.audit_log import log_event

PENDING_PATH = Path("state/multisig_pending.json")

FRIEND_EMAIL = os.getenv("MULTISIG_FRIEND_EMAIL", "")
YUBI_REQUIRED = True  # stub; integrate with actual YubiKey later

def _load_pending() -> Dict[str, Any]:
    try:
        return orjson.loads(PENDING_PATH.read_bytes())
    except Exception:
        return {}

def _save_pending(st: Dict[str, Any]) -> None:
    PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    PENDING_PATH.write_bytes(orjson.dumps(st))

def request_withdrawal(sub_label: str, amount_usdt: float, dest: str) -> str:
    """
    Stage a withdrawal that requires:
      - you / Telegram approval
      - friend email code (out-of-band)
      - YubiKey (stubbed)
    Returns a pending_id.
    """
    st = _load_pending()
    pending_id = secrets.token_hex(8)
    code_friend = secrets.token_hex(4)  # short code

    st[pending_id] = {
        "ts": int(time.time() * 1000),
        "sub_label": sub_label,
        "amount_usdt": amount_usdt,
        "dest": dest,
        "friend_code": code_friend,
        "friend_ok": False,
        "yubi_ok": False,
        "user_ok": False,
    }
    _save_pending(st)

    # TODO: send code_friend to FRIEND_EMAIL via your mail integration
    log_event("withdrawal_requested", {"pending_id": pending_id, "sub_label": sub_label, "amount": amount_usdt, "dest": dest})
    return pending_id

def approve_by_user(pending_id: str) -> None:
    st = _load_pending()
    if pending_id not in st:
        return
    st[pending_id]["user_ok"] = True
    _save_pending(st)
    log_event("withdrawal_user_approved", {"pending_id": pending_id})

def approve_by_friend(pending_id: str, code: str) -> bool:
    st = _load_pending()
    d = st.get(pending_id)
    if not d:
        return False
    if str(d.get("friend_code")) != str(code):
        return False
    d["friend_ok"] = True
    _save_pending(st)
    log_event("withdrawal_friend_approved", {"pending_id": pending_id})
    return True

def approve_by_yubi(pending_id: str) -> None:
    # Stub; hook in actual YubiKey check later
    st = _load_pending()
    if pending_id not in st:
        return
    st[pending_id]["yubi_ok"] = True
    _save_pending(st)
    log_event("withdrawal_yubi_approved", {"pending_id": pending_id})

def ready_to_execute(pending_id: str) -> bool:
    st = _load_pending()
    d = st.get(pending_id)
    if not d:
        return False
    return bool(d.get("user_ok") and d.get("friend_ok") and d.get("yubi_ok"))
