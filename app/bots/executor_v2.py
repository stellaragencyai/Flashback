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

Notes
-----
- This executor is stateless across runs except for the cursor file.
- It does NOT contain strategy rules; those live in config/strategies.yaml
  and core/strategy_gate.py.
"""

from __future__ import annotations

import json
import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any

from app.core.config import settings
from app.core.logger import get_logger, bind_context
from app.core.bybit_client import Bybit
from app.core.notifier_bot import tg_send
from app.core.feature_store import log_features
from app.core.trade_classifier import classify as classify_trade
from app.core.corr_gate import allow as corr_allow
from app.core.sizing import bayesian_size, risk_capped_qty
from app.core.strategy_gate import (
    get_strategies_for_signal,
    is_strategy_enabled,
    is_strategy_live,
    strategy_label,
    strategy_risk_pct,
)
from app.core.portfolio_guard import can_open_trade



log = get_logger("executor_v2")

# Paths
ROOT: Path = settings.ROOT
SIGNAL_FILE: Path = ROOT / "signals" / "observed.jsonl"
CURSOR_FILE: Path = ROOT / "state" / "observed.cursor"

SIGNAL_FILE.parent.mkdir(parents=True, exist_ok=True)
CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)

# Single shared Bybit client (trade key)
_BYBIT_TRADE_CLIENT: Optional[Bybit] = None


# ---------- CURSOR HELPERS ---------- #

def load_cursor() -> int:
    if not CURSOR_FILE.exists():
        return 0
    try:
        return int(CURSOR_FILE.read_text().strip() or "0")
    except Exception:
        return 0


def save_cursor(pos: int) -> None:
    try:
        CURSOR_FILE.write_text(str(pos))
    except Exception as e:
        log.warning("failed to save cursor %s: %r", pos, e)


# ---------- BYBIT CLIENT HELPER ---------- #

def get_trade_client() -> Bybit:
    global _BYBIT_TRADE_CLIENT
    if _BYBIT_TRADE_CLIENT is None:
        _BYBIT_TRADE_CLIENT = Bybit("trade")
    return _BYBIT_TRADE_CLIENT


# ---------- SIGNAL PROCESSOR ---------- #

async def process_signal_line(line: str) -> None:
    try:
        sig = json.loads(line)
    except json.JSONDecodeError:
        log.warning("Invalid JSON in observed.jsonl: %r", line[:200])
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe")
    if not symbol or not tf:
        return

    # Find strategies that want this signal
    strategies = get_strategies_for_signal(symbol, tf)
    if not strategies:
        return

    for strat_name, strat_cfg in strategies.items():
        try:
            await handle_strategy_signal(strat_name, strat_cfg, sig)
        except Exception as e:
            log.exception("Strategy error (%s): %r", strat_name, e)


# ---------- STRATEGY PROCESSOR ---------- #

async def handle_strategy_signal(
    strat_name: str,
    strat_cfg: Dict[str, Any],
    sig: Dict[str, Any],
) -> None:
    bound = bind_context(log, strat=strat_name)

    enabled = bool(strat_cfg.get("enabled", False))
    mode = str(strat_cfg.get("automation_mode", "OFF")).upper().strip()

    if not enabled or mode == "OFF":
        bound.debug("strategy disabled or automation_mode=OFF")
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe")
    side = sig.get("side")
    price = float(sig.get("price", 0) or 0)
    ts = sig.get("ts")

    if not symbol or not tf or not side:
        bound.warning("missing required fields in signal: %r", sig)
        return

    # AI Classifier Gate
    clf = classify_trade(symbol, tf, side, price, ts)
    if not clf.get("allow"):
        bound.info("AI gate rejected: %s", clf.get("reason"))
        return

    # Correlation gate
    if not corr_allow(symbol):
        bound.info("Correlation gate rejected for %s", symbol)
        return

    # Feature logging (best-effort)
    try:
        log_features(symbol, sig, strat_name=strat_name)
    except Exception as e:
        bound.warning("feature logging failed: %r", e)

    # Sizing
    try:
        raw_qty = bayesian_size(symbol, side, price, strat_name)
        qty = risk_capped_qty(symbol, raw_qty)
    except Exception as e:
        bound.error("sizing error: %r", e)
        return

    if not qty or qty <= 0:
        bound.info("qty <= 0 after sizing; skipping entry. raw=%r", raw_qty)
        return

    # Execute
    if mode == "LIVE":
        await execute_entry(symbol, side, qty, price, strat_name, bound)
    else:
        bound.info("PAPER entry: %s %s qty=%s @ ~%s", symbol, side, qty, price)


# ---------- EXECUTOR ---------- #

async def execute_entry(
    symbol: str,
    side: str,
    qty: float,
    price: float,
    strat: str,
    bound_log,
) -> None:
    client = get_trade_client()
    try:
        resp = client.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            qty=qty,
            orderType="Market",
        )
        bound_log.info(
            "LIVE entry executed: %s %s qty=%s resp=%r",
            symbol,
            side,
            qty,
            resp,
        )
        try:
            tg_send(f"🚀 Entry placed [{strat}] {symbol} {side} qty={qty}")
        except Exception as e:
            bound_log.warning("telegram send failed: %r", e)
    except Exception as e:
        bound_log.error("order failed for %s %s qty=%s: %r", symbol, side, qty, e)


# ---------- MAIN LOOP ---------- #

async def executor_loop() -> None:
    pos = load_cursor()
    log.info("executor_v2 starting at cursor=%s", pos)

    while True:
        try:
            if not SIGNAL_FILE.exists():
                await asyncio.sleep(0.5)
                continue

            # Handle file truncation/rotation: if file shrank, reset cursor
            file_size = SIGNAL_FILE.stat().st_size
            if pos > file_size:
                log.info(
                    "Signal file truncated (size=%s, cursor=%s). Resetting cursor to 0.",
                    file_size,
                    pos,
                )
                pos = 0
                save_cursor(pos)

            with SIGNAL_FILE.open("r", encoding="utf-8") as f:
                f.seek(pos)
                for line in f:
                    pos = f.tell()
                    await process_signal_line(line)
                    save_cursor(pos)

            await asyncio.sleep(0.25)

        except Exception as e:
            log.exception("executor loop error: %r; backing off 1s", e)
            await asyncio.sleep(1.0)


# ---------- ENTRYPOINT ---------- #

def main() -> None:
    try:
        asyncio.run(executor_loop())
    except KeyboardInterrupt:
        log.info("executor_v2 stopped by user")


if __name__ == "__main__":
    main()
