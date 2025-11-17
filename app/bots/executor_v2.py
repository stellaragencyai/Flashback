#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Auto Executor v2 (Strategy-aware, multi-sub, AI-gated)

Purpose
-------
- Consume signals from an append-only JSONL file (signals/observed.jsonl).
- For EACH strategy defined in config/strategies.yaml:
    • Check symbol + timeframe match via strategy_gate.
    • Check enabled + automation mode.
    • Run AI gating.
    • Run correlation gate.
    • Size entries.
    • Place live or paper entries.
- TP/SL handled by separate bot (tp_manager).

This file was originally embedded inside supervisor.py because you had
no dedicated executor file. Now it is standalone like a normal person’s codebase.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Dict, Any

from core.config import settings
from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from core.notifier_bot import tg_send

# Feature memory + AI gating + risk logic
from core.feature_store import log_features
from core.trade_classifier import classify as classify_trade
from core.corr_gate import allow as corr_allow
from core.sizing import bayesian_size, risk_capped_qty
from core.strategy_gate import should_strategy_handle

log = get_logger("executor_v2")


# Paths
ROOT: Path = settings.ROOT
SIGNAL_FILE: Path = ROOT / "signals" / "observed.jsonl"
CURSOR_FILE: Path = ROOT / "state" / "observed.cursor"


# ---------- CURSOR HELPERS ----------
def load_cursor() -> int:
    if not CURSOR_FILE.exists():
        return 0
    try:
        return int(CURSOR_FILE.read_text().strip())
    except Exception:
        return 0


def save_cursor(pos: int) -> None:
    CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)
    CURSOR_FILE.write_text(str(pos))


# ---------- SIGNAL PROCESSOR ----------
async def process_signal_line(line: str) -> None:
    try:
        sig = json.loads(line)
    except json.JSONDecodeError:
        log.warning("Invalid JSON in observed.jsonl")
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe")
    if not symbol or not tf:
        return

    # find strategies that want this signal
    strategies = should_strategy_handle(symbol, tf)
    if not strategies:
        return

    for strat_name, strat_cfg in strategies.items():
        try:
            await handle_strategy_signal(strat_name, strat_cfg, sig)
        except Exception as e:
            log.exception(f"Strategy error ({strat_name}): {e}")


# ---------- STRATEGY PROCESSOR ----------
async def handle_strategy_signal(
    strat_name: str, strat_cfg: Dict[str, Any], sig: Dict[str, Any]
) -> None:

    bound = bind_context(log, strat=strat_name)

    enabled = strat_cfg.get("enabled", False)
    mode = strat_cfg.get("automation_mode", "OFF").upper()

    if not enabled or mode == "OFF":
        bound.debug("strategy disabled or OFF")
        return

    symbol = sig["symbol"]
    tf = sig["timeframe"]
    side = sig.get("side")
    price = float(sig.get("price", 0))
    ts = sig.get("ts")

    # AI Classifier Gate
    clf = classify_trade(symbol, tf, side, price, ts)
    if not clf.get("allow"):
        bound.info(f"AI gate rejected: {clf.get('reason')}")
        return

    # Correlation gate
    if not corr_allow(symbol):
        bound.info("Correlation gate rejected")
        return

    # Feature logging
    try:
        log_features(symbol, sig, strat_name=strat_name)
    except Exception as e:
        bound.warning(f"feature logging failed: {e!r}")

    # Sizing
    try:
        raw_qty = bayesian_size(symbol, side, price, strat_name)
        qty = risk_capped_qty(symbol, raw_qty)
    except Exception as e:
        bound.error(f"sizing error: {e!r}")
        return

    # Execute
    if mode == "LIVE":
        await execute_entry(symbol, side, qty, price, strat_name)
        bound.info(f"LIVE entry executed: {symbol} {side} qty={qty}")
    else:
        bound.info(f"PAPER entry: {symbol} {side} qty={qty}")


# ---------- EXECUTOR ----------
async def execute_entry(symbol: str, side: str, qty: float, price: float, strat: str):
    client = Bybit("trade")
    try:
        resp = client.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            qty=qty,
            orderType="Market",
        )
        log.info(f"[{strat}] order placed: {resp}")
        tg_send(f"🚀 Entry placed [{strat}] {symbol} {side} qty={qty}")
    except Exception as e:
        log.error(f"[{strat}] order failed: {e}")


# ---------- MAIN LOOP ----------
async def executor_loop() -> None:
    pos = load_cursor()
    log.info(f"executor_v2 starting at cursor={pos}")

    while True:
        if not SIGNAL_FILE.exists():
            await asyncio.sleep(0.5)
            continue

        with open(SIGNAL_FILE, "r") as f:
            f.seek(pos)
            for line in f:
                pos = f.tell()
                await process_signal_line(line)
                save_cursor(pos)

        await asyncio.sleep(0.25)


# ---------- ENTRYPOINT ----------
def main():
    try:
        asyncio.run(executor_loop())
    except KeyboardInterrupt:
        log.info("executor_v2 stopped by user")


if __name__ == "__main__":
    main()
