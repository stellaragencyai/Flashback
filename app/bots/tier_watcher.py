#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Tier & Milestone Watcher

Watches MAIN equity and sends Telegram alerts when crossing
bootstrap tiers:

Tier1: 25
Tier2: 50
Tier3: 100
Tier4: 250
Tier5: 500
Tier6: 1000
Tier7: 2500
Tier8: 5000
Tier9: 10000
Tier10: 25000

State is stored in: state/tier_watcher.json

Env:
  TIER_WATCHER_POLL_SEC   (default 60)
  TIER_WATCHER_CHAT       (optional override of notifier channel name)
"""

import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, Any

import orjson

try:
    from app.core.config import settings
    from app.core.flashback_common import get_equity_usdt
    from app.core.notifier_bot import get_notifier
except ImportError:
    from core.config import settings  # type: ignore
    from core.flashback_common import get_equity_usdt  # type: ignore
    from core.notifier_bot import get_notifier  # type: ignore


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_PATH: Path = ROOT / "state" / "tier_watcher.json"
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

# Your bootstrap tiers
TIERS = [
    25, 50, 100, 250, 500,
    1000, 2500, 5000, 10000, 25000,
]

POLL_SEC = int(os.getenv("TIER_WATCHER_POLL_SEC", "60"))
NOTIFIER_NAME = os.getenv("TIER_WATCHER_CHAT", "main")

tg = get_notifier(NOTIFIER_NAME)


def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {"last_tier": 0, "last_equity": "0"}
    try:
        return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        return {"last_tier": 0, "last_equity": "0"}


def _save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_bytes(orjson.dumps(state))


def _current_tier(equity: Decimal) -> int:
    lvl = 0
    for t in TIERS:
        if equity >= t:
            lvl += 1
        else:
            break
    return lvl


def loop() -> None:
    state = _load_state()
    last_tier = int(state.get("last_tier", 0))
    last_equity_str = state.get("last_equity", "0")

    tg.info(f"📊 Tier Watcher online. Poll={POLL_SEC}s | last_tier={last_tier} | last_equity={last_equity_str}")

    while True:
        try:
            eq_raw = get_equity_usdt()
            equity = Decimal(str(eq_raw))
        except Exception as e:
            print(f"[TierWatcher] failed to get equity: {e}")
            time.sleep(POLL_SEC)
            continue

            # If equity <= 0, just sleep and continue
        if equity <= 0:
            time.sleep(POLL_SEC)
            continue

        tier = _current_tier(equity)

        if tier != last_tier:
            direction = "UP" if tier > last_tier else "DOWN"
            msg_lines = [
                "🚩 Tier Change Detected",
                "",
                f"📈 Equity: ${equity}",
                f"📶 Tier: {last_tier} → {tier} ({direction})",
            ]
            if tier > last_tier:
                msg_lines.append("")
                msg_lines.append("✅ Milestone reached. Consider lowering risk per plan and seeding subs.")
            else:
                msg_lines.append("")
                msg_lines.append("⚠️ Tier dropped. Review risk and behavior before continuing.")

            tg.info("\n".join(msg_lines))
            last_tier = tier

            state["last_tier"] = last_tier
            state["last_equity"] = str(equity)
            _save_state(state)

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    loop()
