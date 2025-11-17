#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard Bot

What this bot does:
- Loads API keys for MAIN + flashback01..flashback10 from .env
- Builds a WsSwitchboard (multi-account WS orchestrator)
- Subscribes each configured account to:
    • execution
    • position
- Logs incoming events with labels (and can emit limited Telegram messages)

This is the FIRST step:
- Purely observational: it does NOT place orders.
- Meant to prove WebSocket connectivity for all accounts.
- Later, trade_journal, tp_sl_manager, etc., will subscribe to this switchboard
  instead of polling Bybit directly.
"""

from __future__ import annotations

import asyncio
import os
from typing import Dict, Any

from dotenv import load_dotenv

from app.core.config import settings
from app.core.logger import get_logger, bind_context
from app.core.notifier_bot import get_notifier
from app.core.ws_switchboard import WsSwitchboard

log = get_logger("ws_switchboard_bot")
tg = get_notifier("main")

ROOT = settings.ROOT
ENV_PATH = ROOT / ".env"
load_dotenv(ENV_PATH)


def _load_main_creds() -> Dict[str, str]:
    """
    Load MAIN account creds in the same pattern as supervisor.py:
      Preferred: BYBIT_MAIN_API_KEY / BYBIT_MAIN_API_SECRET
      Fallbacks: BYBIT_MAIN_READ_KEY / BYBIT_MAIN_TRADE_KEY, etc.
    """
    key = os.getenv("BYBIT_MAIN_API_KEY")
    sec = os.getenv("BYBIT_MAIN_API_SECRET")

    if not key:
        key = os.getenv("BYBIT_MAIN_READ_KEY") or os.getenv("BYBIT_MAIN_TRADE_KEY")
    if not sec:
        sec = os.getenv("BYBIT_MAIN_READ_SECRET") or os.getenv("BYBIT_MAIN_TRADE_SECRET")

    return {"label": "main", "api_key": key or "", "api_secret": sec or ""}


def _load_sub_creds() -> Dict[str, Dict[str, str]]:
    """
    Load flashback01..flashback10 creds from env:

      BYBIT_FLASHBACK01_API_KEY / BYBIT_FLASHBACK01_API_SECRET
      ...
      BYBIT_FLASHBACK10_API_KEY / BYBIT_FLASHBACK10_API_SECRET
    """
    subs: Dict[str, Dict[str, str]] = {}
    for i in range(1, 11):
        label = f"flashback{i:02d}"
        prefix = f"BYBIT_FLASHBACK{i:02d}"
        key = os.getenv(f"{prefix}_API_KEY", "")
        sec = os.getenv(f"{prefix}_API_SECRET", "")
        subs[label] = {"label": label, "api_key": key, "api_secret": sec}
    return subs


async def log_execution(label: str, row: Dict[str, Any]) -> None:
    """
    Basic execution logger for now.

    Later, trade_journal will hook into this instead of polling.
    """
    b = bind_context(log, acct=label, topic="execution")
    sym = row.get("symbol")
    side = row.get("side")
    qty = row.get("execQty")
    price = row.get("execPrice")
    exec_type = row.get("execType")
    realised = row.get("realisedPnl")

    b.info(
        "WS exec: symbol=%s side=%s qty=%s price=%s type=%s realisedPnl=%s",
        sym,
        side,
        qty,
        price,
        exec_type,
        realised,
    )

    # Optional: very light Telegram ping for first wiring / debugging
    # Comment out if it gets noisy.
    try:
        tg_level = os.getenv("WS_SWITCHBOARD_TG_EXEC_LEVEL", "warn").lower()
        if tg_level == "info":
            tg.info(f"[WS][{label}] exec {sym} {side} qty={qty} px={price} pnl={realised}")
        elif tg_level == "warn":
            # Only warn on non-zero realised pnl
            try:
                if realised not in (None, "", "0", "0.0", "0.0000"):
                    tg.warn(f"[WS][{label}] exec {sym} {side} qty={qty} px={price} pnl={realised}")
            except Exception:
                pass
    except Exception as e:
        b.warning("Telegram exec notify failed: %r", e)


async def log_position(label: str, row: Dict[str, Any]) -> None:
    """
    Basic position logger for now.

    Later, tp_sl_manager can subscribe to these events.
    """
    b = bind_context(log, acct=label, topic="position")
    sym = row.get("symbol")
    size = row.get("size")
    side = row.get("side")
    entry = row.get("avgPrice") or row.get("avgEntryPrice")
    liq = row.get("liqPrice") or row.get("liquidationPrice")

    b.info(
        "WS position: symbol=%s side=%s size=%s entry=%s liq=%s",
        sym,
        side,
        size,
        entry,
        liq,
    )

    # Optional: Telegram on size transitions only later if needed.


async def main_async() -> None:
    log.info("WS Switchboard Bot starting (root=%s, env=%s)", ROOT, ENV_PATH)

    sw = WsSwitchboard()

    # 1) MAIN account
    main_creds = _load_main_creds()
    if main_creds["api_key"] and main_creds["api_secret"]:
        sw.add_account(main_creds["label"], main_creds["api_key"], main_creds["api_secret"])
    else:
        log.warning("MAIN account WS creds missing; no MAIN WS connection will be created.")

    # 2) flashback01..10
    subs = _load_sub_creds()
    for label, cfg in subs.items():
        if cfg["api_key"] and cfg["api_secret"]:
            sw.add_account(label, cfg["api_key"], cfg["api_secret"])
        else:
            log.info("Subaccount %s has no WS creds; skipping.", label)

    # Register handlers
    sw.add_execution_handler(log_execution)
    sw.add_position_handler(log_position)

    # Quick Telegram summary
    try:
        tg.info("📡 WS Switchboard Bot online (accounts: %s)", ", ".join(sorted(sw._clients.keys())))
    except Exception:
        pass

    await sw.run_forever()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("WS Switchboard Bot stopped by user")


if __name__ == "__main__":
    main()
