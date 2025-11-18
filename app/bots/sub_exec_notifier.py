#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Sub Exec Notifier (WS-driven)

Purpose
-------
- Read executions from the WS bus: state/ws_executions.jsonl
  (written by ws_switchboard_bot).
- For every subaccount label (flashback01..flashback10), send clean Telegram
  notifications on executions, with a special ping on FULL LIMIT fills.

Bus format (one line per exec in ws_switchboard_bot):
  {
    "label": "main" | "flashback01" | ...,
    "ts": 1731870000000,
    "row": { ... Bybit execution row ... }
  }

This bot:
- Ignores label == "main" (main is handled by trade_journal + main WS bot).
- Handles only flashbackXX labels.
- Uses notifier_bot.get_notifier(label) so:
    flashback01 → TG_TOKEN_SUB_1 / TG_CHAT_SUB_1
    ...
    flashback10 → TG_TOKEN_SUB_10 / TG_CHAT_SUB_10

Files:
- state/sub_exec.cursor (byte offset into ws_executions.jsonl so no replays)
"""

from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

import orjson

# tolerant imports
try:
    from app.core.config import settings
    from app.core.notifier_bot import get_notifier
except ImportError:
    from core.config import settings  # type: ignore
    from core.notifier_bot import get_notifier  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

BUS_PATH: Path = STATE_DIR / "ws_executions.jsonl"
CURSOR_PATH: Path = STATE_DIR / "sub_exec.cursor"

# use main notifier for startup / errors
tg_main = get_notifier("main")


def _load_cursor_pos() -> int:
    try:
        data = orjson.loads(CURSOR_PATH.read_bytes())
        pos = int(data.get("pos", 0))
        return max(0, pos)
    except Exception:
        return 0


def _save_cursor_pos(pos: int) -> None:
    CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    CURSOR_PATH.write_bytes(orjson.dumps({"pos": int(pos)}))


def _iter_new_bus_payloads(start_pos: int) -> Tuple[List[dict], int]:
    """
    Read new JSONL lines from BUS_PATH starting at start_pos (byte offset).
    Returns (list_of_payloads, new_pos).

    Each payload is the full dict:
      {"label": "...", "ts": ..., "row": {...}}
    """
    if not BUS_PATH.exists():
        return [], start_pos

    try:
        file_size = BUS_PATH.stat().st_size
    except Exception:
        return [], start_pos

    if start_pos > file_size:
        start_pos = 0

    payloads: List[dict] = []
    pos = start_pos

    try:
        with BUS_PATH.open("rb") as f:
            f.seek(start_pos)
            for line in f:
                pos = f.tell()
                if not line.strip():
                    continue
                try:
                    payload = orjson.loads(line)
                except Exception:
                    continue
                payloads.append(payload)
    except Exception:
        # don't kill the bot on file I/O hiccups
        return payloads, pos

    return payloads, pos


def _exec_is_trade(e: dict) -> bool:
    """
    Treat as a trade if:
      - execQty > 0
      - execType contains 'trade' or 'fill' (case-insensitive) or is empty.
    """
    try:
        qty = Decimal(str(e.get("execQty", "0") or "0"))
    except Exception:
        return False
    if qty <= 0:
        return False

    t = str(e.get("execType", "") or "").lower()
    if not t:
        return True
    return ("trade" in t) or ("fill" in t)


def _is_full_limit_fill(e: dict) -> bool:
    """
    Rough full-fill detection:
      - orderType == 'Limit' (case-insensitive)
      - leavesQty == 0 (or close enough)
    """
    otype = str(e.get("orderType", "") or "").lower()
    if otype != "limit":
        return False

    leaves = e.get("leavesQty", "")
    leaves_str = str(leaves) if leaves is not None else ""
    return leaves_str in ("0", "0.0", "0.00", "0.000", "0.0000", "")


def _side_from_exec(e: dict) -> Optional[str]:
    s = e.get("side")
    if s in ("Buy", "Sell"):
        return s
    return None


def loop() -> None:
    tg_main.info("📡 Sub Exec Notifier started (WS-driven for flashback01..flashback10).")

    pos = _load_cursor_pos()

    while True:
        try:
            payloads, new_pos = _iter_new_bus_payloads(pos)
            if new_pos != pos:
                pos = new_pos
                _save_cursor_pos(pos)

            if not payloads:
                time.sleep(1.0)
                continue

            for payload in payloads:
                label = str(payload.get("label", "") or "")
                row = payload.get("row") or {}

                # We only care about subaccounts: flashback01..flashback10
                if not label.startswith("flashback"):
                    continue

                if not _exec_is_trade(row):
                    continue

                sym = row.get("symbol", "?")
                side = _side_from_exec(row) or row.get("side", "?")
                try:
                    qty = Decimal(str(row.get("execQty", "0") or "0"))
                    px = Decimal(str(row.get("execPrice", "0") or "0"))
                except Exception:
                    continue

                realised = row.get("realisedPnl", None)
                notifier = get_notifier(label)

                # Full LIMIT fill → stronger ping
                if _is_full_limit_fill(row):
                    msg = (
                        f"✅ LIMIT filled [{label}] "
                        f"{sym} {side} qty={qty} @ {px} "
                        f"(realisedPnl={realised})"
                    )
                    notifier.trade(msg)
                else:
                    # lighter ping for partials/market/etc
                    msg = (
                        f"⚙️ Exec [{label}] "
                        f"{sym} {side} qty={qty} @ {px} "
                        f"(pnl={realised})"
                    )
                    notifier.info(msg)

            time.sleep(1.0)

        except Exception as e:
            # log via main channel, but don't die
            try:
                tb = repr(e)
                tg_main.error(f"[SubExecNotifier] loop error: {tb}")
            except Exception:
                pass
            time.sleep(5.0)


if __name__ == "__main__":
    loop()
