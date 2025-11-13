# app/core/subs.py
"""
Flashback — Subaccount registry & round-robin

Modernized version:
- NO dependency on app/config/subaccounts.json anymore.
- Subaccounts are derived directly from .env:

    SUB_UID_1=260417078
    SUB_UID_2=...
    ...
    SUB_UID_10=...

- Each UID is mapped to a logical label & notifier channel:

    SUB_UID_1 -> {"uid": "...", "label": "flashback01", "channel": "flashback01"}
    SUB_UID_2 -> {"uid": "...", "label": "flashback02", "channel": "flashback02"}
    ...

This plays nice with:
  - app/core/notifier_bot.py   (channels "flashback01".."flashback10")
  - per_position_drip.py       (expects rr_next() → {"uid", "label"})
  - future bots that want sub routing.

State:
- app/state/subs_state.json keeps the current rr_idx (round-robin index).
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import List, Dict, Optional

from app.core.notifier_bot import get_notifier

STATE_PATH = Path("app/state/subs_state.json")


# ---------- Internal state helpers ----------

def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"rr_idx": 0}


def _save_state(st: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------- Subaccount discovery (from .env) ----------

def load_subs() -> List[Dict]:
    """
    Build the current subaccount list from environment variables.

    For i = 1..10:
      Reads SUB_UID_i.
      If non-empty, creates:

        {
          "uid": "<value of SUB_UID_i>",
          "label": "flashback0i",
          "channel": "flashback0i",
        }

    Example .env fragment:

        SUB_UID_1=260417078
        SUB_UID_2=152304954
        ...
        SUB_UID_6=152499802

    If no SUB_UID_i are set, returns [].
    """
    subs: List[Dict] = []
    for i in range(1, 11):
        uid = os.getenv(f"SUB_UID_{i}", "").strip()
        if not uid:
            continue
        label = f"flashback{i:02d}"
        subs.append(
            {
                "uid": uid,
                "label": label,
                "channel": label,  # for notifier_bot.get_notifier(...)
            }
        )
    return subs


# ---------- Round-robin helpers ----------

def rr_index() -> int:
    """
    Return the current round-robin index (0-based).
    """
    st = _load_state()
    try:
        return int(st.get("rr_idx", 0))
    except Exception:
        return 0


def rr_advance(n: int) -> int:
    """
    Advance the round-robin index by n steps (mod len(subs)).
    Persists the new index to STATE_PATH.
    """
    subs = load_subs()
    if not subs:
        # No subs configured; always index 0 (but effectively unused)
        return 0
    idx = (rr_index() + n) % len(subs)
    _save_state({"rr_idx": idx})
    return idx


def rr_next() -> Optional[Dict]:
    """
    Return the next sub in the round-robin sequence, or None if no subs.

    Typical usage:
        sub = rr_next()
        if sub:
            inter_transfer_usdt_to_sub(sub["uid"], amount)
    """
    subs = load_subs()
    if not subs:
        return None
    idx = rr_advance(1)  # move forward one each call
    return subs[idx]


def peek_current() -> Optional[Dict]:
    """
    Return the current sub for rr_idx without advancing the index.
    """
    subs = load_subs()
    if not subs:
        return None
    idx = rr_index() % len(subs)
    return subs[idx]


# ---------- Telegram helper (via notifier_bot) ----------

def send_tg_to_sub(sub: Dict, text: str) -> None:
    """
    Send a message to the Telegram channel associated with a sub.

    `sub` is expected to be a dict from load_subs()/rr_next()/peek_current(), e.g.:

        {
          "uid": "260417078",
          "label": "flashback01",
          "channel": "flashback01",
        }

    This uses app.core.notifier_bot.get_notifier(channel).
    """
    if not sub:
        return

    channel = sub.get("channel") or sub.get("label")
    if not channel:
        return

    tg = get_notifier(channel)
    tg.info(text)
