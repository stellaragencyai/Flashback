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
- Logs incoming events with labels.
- Sends concise Telegram messages on FULL LIMIT fills (per account).
- Appends EVERY execution row to a JSONL bus:
    state/ws_executions.jsonl

This is the wiring layer:
- Purely observational in terms of market (no order placement).
- Becomes the single source of truth for executions for other bots
  (trade_journal, drip variants, analytics, etc.).
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Dict, Any

from dotenv import load_dotenv
import orjson

from app.core.config import settings
from app.core.logger import get_logger, bind_context
from app.core.notifier_bot import get_notifier
from app.core.ws_switchboard import WsSwitchboard

log = get_logger("ws_switchboard_bot")
tg = get_notifier("main")

ROOT = settings.ROOT
ENV_PATH = ROOT / ".env"
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
EXEC_BUS_PATH = STATE_DIR / "ws_executions.jsonl"

load_dotenv(ENV_PATH)


def _append_exec_to_bus(label: str, row: Dict[str, Any]) -> None:
    """
    Append a single execution event to the central JSONL bus.

    Format per line:
      {
        "label": "main" | "flashback01" | ...,
        "ts": 1731870000000,       # local epoch ms when we processed it
        "row": { ... raw Bybit row ... }
      }
    """
    try:
        payload = {
            "label": label,
            "ts": int(time.time() * 1000),
            "row": row,
        }
        with EXEC_BUS_PATH.open("ab") as f:
            f.write(orjson.dumps(payload))
            f.write(b"\n")
    except Exception as e:
        # This must NEVER kill the WS loop; just log it.
        log.warning("Failed to append exec to bus: %r", e)


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
    Core execution handler:

    - Logs to ws_switchboard logger.
    - Appends to state/ws_executions.jsonl (central bus).
    - Sends Telegram on full LIMIT fills.
    - Optional extra exec stream via WS_SWITCHBOARD_TG_EXEC_LEVEL.
    """
    b = bind_context(log, acct=label, topic="execution")
    sym = row.get("symbol")
    side = row.get("side")
    qty = row.get("execQty")
    price = row.get("execPrice")
    exec_type = row.get("execType")
    realised = row.get("realisedPnl")

    order_type = (row.get("orderType") or "").lower()
    leaves_qty_raw = row.get("leavesQty", "")
    leaves_qty_str = str(leaves_qty_raw) if leaves_qty_raw is not None else ""
    # crude "zero" detection, Bybit loves strings
    is_zero_leaves = leaves_qty_str in ("0", "0.0", "0.00", "0.000", "0.0000", "")

    b.info(
        "WS exec: symbol=%s side=%s qty=%s price=%s type=%s realisedPnl=%s "
        "orderType=%s leavesQty=%s",
        sym,
        side,
        qty,
        price,
        exec_type,
        realised,
        order_type,
        leaves_qty_str,
    )

    # Append to bus (fire-and-forget)
    _append_exec_to_bus(label, row)

    # --- FULL LIMIT FILL DETECTION ----------------------------------------
    # We consider it "full" if:
    #   - orderType == "limit"
    #   - leavesQty is effectively zero
    is_full_limit_fill = order_type == "limit" and is_zero_leaves

    if is_full_limit_fill:
        exec_type_str = (exec_type or "").lower()
        # You can filter on exec_type_str if Bybit gets cute; for now we accept most.
        try:
            msg = (
                f"✅ LIMIT filled [{label}] "
                f"{sym} {side} qty={qty} @ {price} "
                f"(realisedPnl={realised})"
            )
            # Use trade() so it gets the 💹 prefix in main stream
            tg.trade(msg)
        except Exception as e:
            b.warning("Telegram full-fill notify failed: %r", e)

    # --- Optional extra notifications -------------------------------------
    # Controlled via WS_SWITCHBOARD_TG_EXEC_LEVEL:
    #   none  (default) -> only full-fill pings
    #   warn           -> pings on non-zero realisedPnl
    #   info           -> pings on every execution
    try:
        tg_level = os.getenv("WS_SWITCHBOARD_TG_EXEC_LEVEL", "none").lower()
        if tg_level == "info":
            tg.info(f"[WS][{label}] exec {sym} {side} qty={qty} px={price} pnl={realised}")
        elif tg_level == "warn":
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
    # No Telegram spam here (yet). We'll add smarter logic later if needed.


async def main_async() -> None:
    log.info("WS Switchboard Bot starting (root=%s, env=%s)", ROOT, ENV_PATH)
    log.info("Execution bus path: %s", EXEC_BUS_PATH)

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
        accounts_list = ", ".join(sorted(sw._clients.keys())) or "NONE"
        tg.info(f"📡 WS Switchboard Bot online (accounts: {accounts_list})")
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
