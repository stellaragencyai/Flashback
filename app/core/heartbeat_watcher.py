#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Heartbeat Watcher

Monitors core bots and alerts if they go silent for too long.

Expected:
  - Other bots periodically call heartbeat.touch("tp_manager"), etc.
"""

import time
from pathlib import Path
from typing import Dict

import orjson

try:
    from app.core.config import settings
    from app.core.notifier_bot import get_notifier
except ImportError:
    from core.config import settings  # type: ignore
    from core.notifier_bot import get_notifier  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR = ROOT / "state"
HB_PATH = STATE_DIR / "heartbeats.json"

tg = get_notifier("main")

# seconds
THRESHOLDS = {
    "tp_manager": 60,
    "trade_journal": 60,
    "executor_v2": 60,
    "portfolio_guard": 300,  # if it ever heartbeats
}

def _load_hb() -> Dict[str, float]:
    if not HB_PATH.exists():
        return {}
    try:
        return orjson.loads(HB_PATH.read_bytes())
    except Exception:
        return {}


def _save_hb(data: Dict[str, float]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    HB_PATH.write_bytes(orjson.dumps(data))


def loop():
    tg.info("💓 Heartbeat Watcher online.")
    while True:
        try:
            hb = _load_hb()
            now = time.time()

            for name, threshold in THRESHOLDS.items():
                last = hb.get(name)
                if last is None:
                    continue
                delta = now - float(last)
                if delta > threshold:
                    msg = (
                        f"⚠️ Bot silent: {name}\n\n"
                        f"⏱ Last heartbeat: {int(delta)}s ago.\n"
                        f"Threshold: {threshold}s."
                    )
                    tg.warn(msg)
                    # after warning once, bump last time so we don't spam
                    hb[name] = now
                    _save_hb(hb)

            time.sleep(15)
        except Exception as e:
            print(f"[HeartbeatWatcher] error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    while True:
        try:
            loop()
        except Exception as e:
            print(f"[HeartbeatWatcher] crashed: {e}")
            time.sleep(10)
