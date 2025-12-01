#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Health Watcher Bot (v1)

Purpose
-------
Central watcher for all bot heartbeats written via flashback_common.record_heartbeat().

What it does:
- Periodically scans state/heartbeats/*.json for last heartbeat times.
- For each configured bot:
    • If no heartbeat file: status = MISSING
    • If heartbeat older than STALE_SEC: status = STALE
    • Otherwise: status = OK
- Sends Telegram alerts ONLY on status transitions (OK -> STALE, STALE -> OK, etc.)
- Optionally writes a simple breaker flag to state/global_breaker.json when
  any "critical" bot is STALE or MISSING.

Config via .env
---------------
HEALTH_BOTS                -> comma-separated list of bot names to monitor
                              e.g. "tp_sl_manager,ws_switchboard_bot,trade_journal,profit_sweeper"
HEALTH_CRITICAL_BOTS       -> subset for breaker logic (comma-separated)
                              e.g. "tp_sl_manager,ws_switchboard_bot"
HEALTH_STALE_SEC           -> seconds after which a bot is considered STALE (default 45)
HEALTH_POLL_SEC            -> how often to rescan (default 10)
HEALTH_BREAKER_ENABLED     -> "true"/"false" (default true)
HEALTH_BREAKER_FILE        -> override path for breaker file (default state/global_breaker.json)

Notes
-----
- Heartbeat files are expected at: state/heartbeats/{bot_name}.json
- Format is assumed to be: {"ts": <epoch_ms or epoch_s>}
  If ts > 1e12 -> interpreted as milliseconds, else seconds.
- This bot does not kill anything; it only:
    • writes breaker flag
    • spams you on Telegram when stuff dies or comes back
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import orjson

from app.core.config import settings
from app.core.notifier_bot import get_notifier

ROOT = settings.ROOT
STATE_DIR = ROOT / "state"
HEARTBEAT_DIR = STATE_DIR / "heartbeats"
HEARTBEAT_DIR.mkdir(parents=True, exist_ok=True)

# Breaker file
DEFAULT_BREAKER_PATH = STATE_DIR / "global_breaker.json"

# Telegram notifier (main stream for now; we can dedicate a channel later)
tg = get_notifier("main")


def _parse_bool(val: Optional[str], default: bool) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _parse_csv(val: Optional[str]) -> Tuple[str, ...]:
    if not val:
        return tuple()
    return tuple(sorted(set(s.strip() for s in val.split(",") if s.strip())))


HEALTH_BOTS = _parse_csv(os.getenv("HEALTH_BOTS"))
HEALTH_CRITICAL_BOTS = _parse_csv(os.getenv("HEALTH_CRITICAL_BOTS"))

try:
    HEALTH_STALE_SEC = int(os.getenv("HEALTH_STALE_SEC", "45"))
except Exception:
    HEALTH_STALE_SEC = 45

try:
    HEALTH_POLL_SEC = int(os.getenv("HEALTH_POLL_SEC", "10"))
except Exception:
    HEALTH_POLL_SEC = 10

HEALTH_BREAKER_ENABLED = _parse_bool(os.getenv("HEALTH_BREAKER_ENABLED"), True)
BREAKER_PATH = Path(os.getenv("HEALTH_BREAKER_FILE", str(DEFAULT_BREAKER_PATH)))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_heartbeat(bot: str) -> Optional[int]:
    """
    Load heartbeat ts for a given bot.

    Returns:
        ts_ms (int) or None if file missing/invalid.
    """
    path = HEARTBEAT_DIR / f"{bot}.json"
    if not path.exists():
        return None

    try:
        data = orjson.loads(path.read_bytes())
    except Exception:
        return None

    ts = data.get("ts")
    if ts is None:
        return None

    try:
        ts_val = int(ts)
    except Exception:
        return None

    # Heuristic: > 1e12 -> ms, else seconds
    if ts_val < 10**11:
        ts_val *= 1000
    return ts_val


def _write_breaker(tripped: bool, reason: str) -> None:
    """
    Write a simple breaker flag.

    {
      "tripped": true/false,
      "reason": "...",
      "ts": <epoch_ms>
    }
    """
    if not HEALTH_BREAKER_ENABLED:
        return

    try:
        payload = {
            "tripped": bool(tripped),
            "reason": reason,
            "ts": _now_ms(),
        }
        BREAKER_PATH.parent.mkdir(parents=True, exist_ok=True)
        BREAKER_PATH.write_bytes(orjson.dumps(payload))
    except Exception:
        # Do not crash the watcher over this.
        pass


def _status_summary_line(name: str, status: str, age_sec: Optional[int]) -> str:
    if age_sec is None:
        return f"{name}: {status} (no heartbeat)"
    return f"{name}: {status} (last {age_sec}s ago)"


def loop() -> None:
    if not HEALTH_BOTS:
        tg.warn(
            "⚠️ Health Watcher started but HEALTH_BOTS is empty. "
            "No bots will be monitored. Set HEALTH_BOTS in .env."
        )
        # Still idle, in case you want to hot-reload env and restart later.
        while True:
            time.sleep(60)

    tg.info(
        "🩺 Health Watcher online.\n"
        f"Monitoring: {', '.join(HEALTH_BOTS)}\n"
        f"Critical: {', '.join(HEALTH_CRITICAL_BOTS) or 'NONE'}\n"
        f"Stale after: {HEALTH_STALE_SEC}s | Poll: {HEALTH_POLL_SEC}s\n"
        f"Breaker: {'ON' if HEALTH_BREAKER_ENABLED else 'OFF'}"
    )

    last_status: Dict[str, str] = {}  # bot -> "OK" | "STALE" | "MISSING"
    last_breaker_state: Optional[bool] = None

    while True:
        try:
            now_ms = _now_ms()
            any_critical_bad = False
            lines = []

            for bot in HEALTH_BOTS:
                ts = _load_heartbeat(bot)

                if ts is None:
                    status = "MISSING"
                    age_sec = None
                else:
                    age_ms = max(0, now_ms - ts)
                    age_sec = int(age_ms / 1000)
                    status = "OK" if age_sec <= HEALTH_STALE_SEC else "STALE"

                prev = last_status.get(bot)
                if prev != status:
                    # Status transition -> Telegram notification
                    if status == "OK":
                        tg.info(f"✅ {bot} heartbeat recovered ({age_sec}s ago).")
                    elif status == "STALE":
                        tg.warn(f"🟠 {bot} heartbeat STALE (last seen {age_sec}s ago).")
                    elif status == "MISSING":
                        tg.error(f"🔴 {bot} heartbeat MISSING (no file).")

                last_status[bot] = status
                lines.append(_status_summary_line(bot, status, age_sec))

                if bot in HEALTH_CRITICAL_BOTS and status in ("STALE", "MISSING"):
                    any_critical_bad = True

            # Breaker logic
            if HEALTH_CRITICAL_BOTS:
                if any_critical_bad and last_breaker_state is not True:
                    _write_breaker(True, "critical bot stale/missing")
                    last_breaker_state = True
                    tg.error("⛔ Global breaker TRIPPED due to critical bot failure.")
                elif not any_critical_bad and last_breaker_state is not False:
                    _write_breaker(False, "all critical bots healthy")
                    last_breaker_state = False
                    tg.info("✅ Global breaker RESET (all critical bots healthy).")

            # Optional periodic summary (every loop for now; can throttle later)
            summary = "🩺 Health status:\n" + "\n".join(f"• {ln}" for ln in lines)
            print(summary, flush=True)

        except Exception as e:
            # Don't die; just log locally and keep going
            print(f"[health_watcher] ERROR: {e}", flush=True)

        time.sleep(HEALTH_POLL_SEC)


if __name__ == "__main__":
    loop()
