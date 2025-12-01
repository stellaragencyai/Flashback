#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Session Guard

Role
----
Tracks session-level psychology/risk constraints, such as:
- Max trades per day
- Max consecutive losses
- Basic per-day stats (wins/losses/PnL count if you want later)

Journal calls:
    session_guard.register_trade_result("WIN" | "LOSS" | "BREAKEVEN" | "UNKNOWN")

Executor (later) can call:
    session_guard.should_block_trading() -> bool

State is persisted in: state/session_guard.json

Env knobs (optional):
    SESSION_MAX_TRADES_PER_DAY   (default 999)
    SESSION_MAX_LOSS_STREAK      (default 5)
"""

import os
import json
import time
from pathlib import Path
from typing import Dict, Any

from datetime import datetime

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH: Path = STATE_DIR / "session_guard.json"

# ---------- config via env ----------

def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


SESSION_MAX_TRADES_PER_DAY = _env_int("SESSION_MAX_TRADES_PER_DAY", 999)
SESSION_MAX_LOSS_STREAK = _env_int("SESSION_MAX_LOSS_STREAK", 5)


# ---------- helpers ----------

def _today_str() -> str:
    # Local calendar day (whatever your OS timezone is set to)
    return datetime.now().strftime("%Y-%m-%d")


def _default_state() -> Dict[str, Any]:
    return {
        "day": _today_str(),
        "trades_today": 0,
        "wins_today": 0,
        "losses_today": 0,
        "breakeven_today": 0,
        "loss_streak": 0,
        "win_streak": 0,
        "last_update_ts": int(time.time()),
    }


def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return _default_state()
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return _default_state()

    # Day rollover: if stored day != today, reset counters
    today = _today_str()
    if data.get("day") != today:
        data = _default_state()
        data["day"] = today
    return data


def _save_state(state: Dict[str, Any]) -> None:
    state["last_update_ts"] = int(time.time())
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------- public API ----------

def get_state() -> Dict[str, Any]:
    """
    Returns current session state (after possible day rollover).
    """
    return _load_state()


def register_trade_result(result: str) -> None:
    """
    Update guard state when a trade fully closes.

    result: "WIN" | "LOSS" | "BREAKEVEN" | anything else
    """
    state = _load_state()

    res = (result or "UNKNOWN").upper()
    state["trades_today"] = int(state.get("trades_today", 0)) + 1

    if res == "WIN":
        state["wins_today"] = int(state.get("wins_today", 0)) + 1
        state["win_streak"] = int(state.get("win_streak", 0)) + 1
        state["loss_streak"] = 0
    elif res == "LOSS":
        state["losses_today"] = int(state.get("losses_today", 0)) + 1
        state["loss_streak"] = int(state.get("loss_streak", 0)) + 1
        state["win_streak"] = 0
    elif res == "BREAKEVEN":
        state["breakeven_today"] = int(state.get("breakeven_today", 0)) + 1
        # streaks unchanged
    else:
        # Unknown result, don't touch streaks but still count as a trade.
        pass

    _save_state(state)


def should_block_trading() -> bool:
    """
    Returns True if trading *should* be blocked due to session rules.

    Executor can call this before opening a new trade and respect it.
    """
    state = _load_state()
    trades_today = int(state.get("trades_today", 0))
    loss_streak = int(state.get("loss_streak", 0))

    if trades_today >= SESSION_MAX_TRADES_PER_DAY:
        return True

    if loss_streak >= SESSION_MAX_LOSS_STREAK:
        return True

    return False


def summary_str() -> str:
    """
    Human-readable snapshot, can be used for Telegram / debugging.
    """
    state = _load_state()
    return (
        f"SessionGuard {state.get('day')} | "
        f"trades={state.get('trades_today', 0)}, "
        f"wins={state.get('wins_today', 0)}, "
        f"losses={state.get('losses_today', 0)}, "
        f"BE={state.get('breakeven_today', 0)}, "
        f"loss_streak={state.get('loss_streak', 0)}, "
        f"win_streak={state.get('win_streak', 0)}"
    )
