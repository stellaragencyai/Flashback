#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Subaccount Registry & Round-Robin

Purpose:
    Central place to understand which subaccounts exist, what they are called,
    and how to rotate through them for DRIP / profit distribution / canary tests.

Reads from .env:
    SUB_UID_1..SUB_UID_10     -> individual sub UIDs (Bybit MemberId)
    SUB_UIDS_ROUND_ROBIN      -> comma list of UIDs for rotation
    SUB_LABELS                -> "uid:Label,uid:Label,..." mapping

State:
    state/subs_rr.json:
        {
          "index": <int>   # pointer into current round-robin list
        }

API:
    all_subs()      -> List[{"uid": str, "label": str, "enabled": bool}]
    rr_next()       -> {"uid": str, "label": str} | None
    peek_current()  -> {"uid": str, "label": str} | None
    reset_rr()      -> None   (reset index to 0)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Dict, Optional, Any

# ROOT = project root: .../Flashback
ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

RR_STATE_PATH = STATE_DIR / "subs_rr.json"

# ---- Env parsing ----

# Collect SUB_UID_1..SUB_UID_10 (if present)
_env = os.environ
_sub_uids_raw: List[str] = []
for i in range(1, 11):
    val = _env.get(f"SUB_UID_{i}")
    if val:
        val = val.strip()
        if val:
            _sub_uids_raw.append(val)

# Fallback/explicit rotation list
_rr_env = _env.get("SUB_UIDS_ROUND_ROBIN", "")
_rr_uids: List[str] = [
    x.strip() for x in _rr_env.split(",") if x.strip()
]

# If no explicit RR list, use SUB_UID_1..N
if not _rr_uids and _sub_uids_raw:
    _rr_uids = list(_sub_uids_raw)

# Label mapping from SUB_LABELS env
# Example:
#   SUB_LABELS=524630315:Sub1_Trend,524633243:Sub2_Breakout,...
_labels_raw = _env.get("SUB_LABELS", "")

_label_map: Dict[str, str] = {}
if _labels_raw:
    for chunk in _labels_raw.split(","):
        chunk = chunk.strip()
        if not chunk or ":" not in chunk:
            continue
        uid_str, label = chunk.split(":", 1)
        uid_str = uid_str.strip()
        label = label.strip()
        if not uid_str or not label:
            continue
        _label_map[uid_str] = label


def _label_for_uid(uid: str) -> str:
    """
    Get a human-readable label for a sub UID.
    Uses SUB_LABELS if provided; falls back to "sub-<uid>".
    """
    uid_s = str(uid)
    return _label_map.get(uid_s, f"sub-{uid_s}")


# ---- State helpers ----

def _load_rr_state() -> Dict[str, Any]:
    try:
        if RR_STATE_PATH.exists():
            return json.loads(RR_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"index": 0}


def _save_rr_state(st: Dict[str, Any]) -> None:
    RR_STATE_PATH.write_text(
        json.dumps(st, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )


# ---- Public API ----

def all_subs() -> List[Dict[str, Any]]:
    """
    Return a list of all known subs based on SUB_UID_1..N,
    with labels attached. All marked enabled=True by default
    (manual-only logic is handled at a higher level by strategies).
    """
    uids = _sub_uids_raw or _rr_uids
    subs: List[Dict[str, Any]] = []
    for uid in uids:
        subs.append(
            {
                "uid": uid,
                "label": _label_for_uid(uid),
                "enabled": True,  # strategy-level config decides true/false behaviour
            }
        )
    return subs


def _rr_list() -> List[Dict[str, str]]:
    """
    Internal: build the rotation list as [{"uid": .., "label": ..}, ...].
    Uses SUB_UIDS_ROUND_ROBIN if set, else SUB_UID_1..N.
    """
    uids = _rr_uids or _sub_uids_raw
    out: List[Dict[str, str]] = []
    for uid in uids:
        out.append(
            {
                "uid": uid,
                "label": _label_for_uid(uid),
            }
        )
    return out


def rr_next() -> Optional[Dict[str, str]]:
    """
    Get the next sub in the round-robin rotation and advance the pointer.

    Returns:
        {"uid": <str>, "label": <str>} or None if no subs configured.
    """
    rr = _rr_list()
    if not rr:
        return None

    st = _load_rr_state()
    idx = int(st.get("index", 0))
    if idx < 0 or idx >= len(rr):
        idx = 0

    sub = rr[idx]

    # advance index
    idx = (idx + 1) % len(rr)
    st["index"] = idx
    _save_rr_state(st)

    return sub


def peek_current() -> Optional[Dict[str, str]]:
    """
    Peek at the current RR sub without advancing.

    Returns:
        {"uid": <str>, "label": <str>} or None if no subs configured.
    """
    rr = _rr_list()
    if not rr:
        return None

    st = _load_rr_state()
    idx = int(st.get("index", 0))
    if idx < 0 or idx >= len(rr):
        idx = 0

    return rr[idx]


def reset_rr() -> None:
    """
    Reset the round-robin index to 0.
    Useful in tests or when re-seeding rotation.
    """
    _save_rr_state({"index": 0})
