#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Auto Executor v2 (Strategy-aware, multi-sub, AI-gated)

Purpose
-------
- Consume signals from an append-only JSONL file (signals/observed.jsonl).
- For EACH strategy defined in config/strategies.yaml:
    • Check symbol + timeframe match via strategy registry (app.core.strategies).
    • Check enabled + automation mode (OFF / LEARN_DRY / LIVE_CANARY / LIVE_FULL).
    • Run AI gating (trade_classifier) — soft-fail, never crash executor.
    • Run correlation gate.
    • Size entries using a simple % of equity per strategy.
    • Log rich feature context via feature_store.log_features for AI memory.
    • Place live entries only for LIVE_* strategies, tagging each order
      with a strategy-aware orderLinkId so downstream bots can attribute
      PnL / risk to the correct logical subaccount.
- TP/SL handled by separate bot (tp_manager).

Notes
-----
- This executor is stateless across runs except for the cursor file.
- Strategy rules live in config/strategies.yaml and app.core.strategies.
"""

from __future__ import annotations

import json
import asyncio
import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, List, Any

from app.core.config import settings
from app.core.logger import get_logger, bind_context
from app.core.bybit_client import Bybit
from app.core.notifier_bot import tg_send
from app.core.feature_store import log_features
from app.core.trade_classifier import classify as classify_trade
from app.core.corr_gate_v2 import allow as corr_allow
from app.core.portfolio_guard import can_open_trade
from app.core.flashback_common import get_equity_usdt

# Strategy registry
from app.core.strategies import ai_strategies_for_signal, Strategy

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


# ---------- ORDER TAGGING ---------- #

def build_order_link_id(strat: Strategy, symbol: str, mode: str) -> str:
    """
    Build a strategy-aware orderLinkId, so we can attribute trades later.

    Format (<= 36 chars for Bybit safety), e.g.:
        FBv2-S2-BTC-1731901234567

    Where:
      - "FBv2"         : Flashback v2 tag
      - "S2"           : short strategy index or sub_uid tail
      - "BTC"          : symbol prefix
      - timestamp_ms   : uniqueness
    """
    ts = int(time.time() * 1000)

    # Try to build a short strategy marker
    sub_suffix = str(strat.sub_uid)[-3:] if strat.sub_uid else "XXX"
    # symbol prefix (BTC from BTCUSDT)
    sym_prefix = symbol.replace("USDT", "").replace("USDC", "")[:4]

    base = f"FBv2-S{sub_suffix}-{sym_prefix}-{ts}"
    # Hard truncate to 36 chars to fit Bybit's orderLinkId constraints
    return base[:36]


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

    # Find strategies that want this signal (LEARN_DRY + LIVE_*)
    strategies: List[Strategy] = ai_strategies_for_signal(symbol, tf)
    if not strategies:
        return

    for strat in strategies:
        try:
            await handle_strategy_signal(strat, sig)
        except Exception as e:
            log.exception("Strategy error (%s): %r", strat.id, e)


# ---------- STRATEGY PROCESSOR ---------- #

async def handle_strategy_signal(
    strat: Strategy,
    sig: Dict[str, Any],
) -> None:
    """
    Handle a single signal for a single Strategy object.

    Modes:
      - OFF        -> ignored
      - LEARN_DRY  -> AI + sizing + feature logging, PAPER only
      - LIVE_CANARY / LIVE_FULL -> AI + sizing + feature logging + LIVE orders
    """
    bound = bind_context(log, strat=strat.id)

    if not strat.enabled:
        bound.debug("strategy disabled (enabled=false)")
        return

    mode = str(strat.automation_mode or "OFF").upper().strip()
    if mode == "OFF":
        bound.debug("automation_mode=OFF; ignoring signal")
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe")
    side = sig.get("side")
    price_val = sig.get("price", 0) or 0
    price = float(price_val)

    if not symbol or not tf or not side:
        bound.warning("missing required fields in signal: %r", sig)
        return

    # --- AI Classifier Gate (soft-fail) ---
    # Try to use trade_classifier.classify, but never allow it to kill the executor.
    try:
        clf = classify_trade(sig, strat.id)
        if not isinstance(clf, dict):
            raise TypeError(f"classifier returned {type(clf)}")
    except Exception as e:
        bound.warning(
            "AI classifier crashed or misbehaved for [%s]: %r — bypassing gate (allow=True).",
            strat.id,
            e,
        )
        clf = {"allow": True, "reason": f"ai_error_bypassed: {e}"}

    if not clf.get("allow"):
        bound.info("AI gate rejected: %s", clf.get("reason"))
        return

    # --- Correlation gate ---
    try:
        allowed_corr, corr_reason = corr_allow(symbol)
    except Exception as e:
        bound.warning("Correlation gate error for %s: %r; bypassing.", symbol, e)
        allowed_corr, corr_reason = True, "corr_gate_v2 exception, bypassed"

    if not allowed_corr:
        bound.info("Correlation gate rejected for %s: %s", symbol, corr_reason)
        return

    # --- Sizing: simple % of equity per strategy ---
    try:
        if price <= 0:
            bound.info("price <= 0; skipping sizing for %s", symbol)
            return

        try:
            equity_usd = Decimal(str(get_equity_usdt()))
        except Exception as e:
            bound.error("get_equity_usdt error: %r", e)
            return

        risk_pct_val = Decimal(str(strat.risk_per_trade_pct or 0))
        if risk_pct_val <= 0:
            bound.info(
                "risk_per_trade_pct <= 0 for %s; skipping entry.", strat.id
            )
            return

        # Interpret risk_per_trade_pct as percent, e.g. 0.25 -> 0.25%
        risk_frac = risk_pct_val / Decimal("100")
        notional_usd = (equity_usd * risk_frac).quantize(Decimal("0.01"))

        if notional_usd <= 0:
            bound.info(
                "notional_usd <= 0 after sizing for %s; equity=%s risk_pct=%s",
                strat.id,
                equity_usd,
                risk_pct_val,
            )
            return

        qty = (notional_usd / Decimal(str(price))).quantize(Decimal("0.0001"))
    except Exception as e:
        bound.error("sizing error: %r", e)
        return

    if qty <= 0:
        bound.info("qty <= 0 after sizing; skipping entry. notional=%s", notional_usd)
        return

    # --- Feature logging for AI memory (best-effort) ---
    try:
        ai_score = clf.get("score") or clf.get("confidence") or 1.0
        ai_reason = str(clf.get("reason", ""))
        features = clf.get("features") or {}

        log_features(
            sub_uid=str(strat.sub_uid),
            strategy=strat.role or strat.name,
            strategy_id=strat.id,
            symbol=symbol,
            side=side,
            mode=mode,
            equity_usd=equity_usd,
            risk_usd=notional_usd,
            risk_pct=risk_pct_val,
            ai_score=float(ai_score),
            ai_reason=ai_reason,
            features=features,
            signal=sig,
        )
    except Exception as e:
        bound.warning("feature logging failed: %r", e)

    # LIVE vs PAPER behaviour based on strategy.automation_mode
    if strat.can_trade_live:
        order_link_id = build_order_link_id(strat, symbol, mode)
        await execute_entry(symbol, side, float(qty), price, strat, mode, order_link_id, bound)
    else:
        bound.info(
            "PAPER entry (%s): %s %s qty=%s @ ~%s",
            mode,
            symbol,
            side,
            qty,
            price,
        )


# ---------- EXECUTOR ---------- #

async def execute_entry(
    symbol: str,
    side: str,
    qty: float,
    price: float,
    strat: Strategy,
    mode: str,
    order_link_id: str,
    bound_log,
) -> None:
    """
    Place a LIVE order for a given strategy.

    Uses existing Bybit client and places a linear Market order.
    Tags each order with orderLinkId so downstream bots / journaling
    can attribute it back to (sub_uid, strategy_id).
    """
    # Optional: portfolio guard hook
    try:
        if not can_open_trade(symbol):
            bound_log.info("Portfolio guard blocked new trade on %s", symbol)
            return
    except TypeError:
        # Signature mismatch, ignore for now rather than crash
        pass
    except Exception as e:
        bound_log.warning("portfolio_guard.can_open_trade error: %r; continuing without it", e)

    client = get_trade_client()
    try:
        resp = client.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            qty=qty,
            orderType="Market",
            orderLinkId=order_link_id,
        )
        bound_log.info(
            "LIVE entry executed [%s | mode=%s]: %s %s qty=%s linkId=%s resp=%r",
            strat.id,
            mode,
            symbol,
            side,
            qty,
            order_link_id,
            resp,
        )
        try:
            tg_send(
                f"🚀 Entry placed [{strat.id}] {symbol} {side} qty={qty} "
                f"mode={mode} linkId={order_link_id}"
            )
        except Exception as e:
            bound_log.warning("telegram send failed: %r", e)
    except Exception as e:
        bound_log.error(
            "order failed for [%s] %s %s qty=%s linkId=%s: %r",
            strat.id,
            symbol,
            side,
            qty,
            order_link_id,
            e,
        )


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

            # Read in binary mode so tell() is allowed during iteration
            with SIGNAL_FILE.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()

                    try:
                        line = raw.decode("utf-8").strip()
                    except Exception as e:
                        log.warning("executor_v2: failed to decode line at pos=%s: %r", pos, e)
                        continue

                    if not line:
                        continue

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
