#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Auto Executor v2 (Strategy-aware, multi-sub, AI-gated + canary sizing + guard-aware + feature logging)

Purpose
-------
- Consume signals from an append-only JSONL file (signals/observed.jsonl).
- For EACH strategy defined in config/strategies.yaml:
    • Decide if that strategy should care about the signal
      (symbol + timeframe match via strategy_gate).
    • Check `enabled` and `automation_mode` (OFF / PAPER / LEARN_DRY / LIVE_CANARY / LIVE_FULL).
    • Run the candidate through the AI gate (ai_gate_decide).
    • If allowed, size and place entry orders on Bybit (MAIN unified account),
      subject to Portfolio Guard (daily loss limits & per-trade risk caps).
    • Log a feature snapshot to the Feature Store for AI memory.
- Let TP/SL Manager handle exits via ladders / stop logic.

Key ideas
---------
- Strategies are defined in config/strategies.yaml and accessed via:
      app.core.strategy_gate
- This executor is *global*: one process can drive multiple strategies/sub_uids.
- Execution happens on MAIN account for now (unified / linear perps).
  Strategies are "logical" owners (sub_uid) used for AI + logging.

Signals (JSONL) expected shape (minimal):
    {
        "symbol": "BTCUSDT",
        "side": "Buy",            # or "Sell"
        "timeframe": "5m",        # optional but recommended
        "reason": "trend_breakout",
        "ts_ms": 1731712345678,
        "est_rr": 0.25,           # optional, for AI gate
        "stop_price": 62000.0,    # optional, for future sizing
        "entry_hint": "market"    # optional: "market" or "limit"
    }

Anything extra is ignored.

.env
----
    EXEC_ENABLED=true|false               # master on/off (default true)
    EXEC_DRY_RUN=true|false               # if true, NEVER send live orders
    EXEC_SIGNALS_PATH=signals/observed.jsonl
    EXEC_POLL_SECONDS=2
    EXEC_DEFAULT_RISK_PCT=0.25            # fallback if strategy has no risk_per_trade_pct
    EXEC_SUB_FILTER=524630315,524633243   # optional list of sub_uids to act for
    EXEC_CANARY_MAX_NOTIONAL=5            # USDT cap per trade for LIVE_CANARY mode

Dependencies
------------
    - app.core.flashback_common:
        bybit_post, get_equity_usdt, list_open_positions, last_price
    - app.core.strategy_gate:
        get_strategies_for_signal, all_strategies,
        is_strategy_enabled, is_strategy_off, is_strategy_live, is_strategy_paper,
        strategy_risk_pct, strategy_max_concurrent, strategy_label
    - app.ai.executor_ai_gate:
        ai_gate_decide
    - app.core.notifier_bot:
        get_notifier("main")
    - app.core.portfolio_guard:
        can_open_trade
    - app.core.feature_store:
        log_trade_open

State
-----
    - state/auto_executor.json:
        { "last_offset": <int> }  # byte offset in signals file; used to resume
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

from app.core.flashback_common import (
    bybit_post,
    get_equity_usdt,
    list_open_positions,
    last_price,
)

from app.core.strategy_gate import (
    get_strategies_for_signal,
    all_strategies,
    is_strategy_enabled,
    is_strategy_off,
    is_strategy_live,
    is_strategy_paper,
    strategy_risk_pct,
    strategy_max_concurrent,
    strategy_label,
)

from app.ai.executor_ai_gate import ai_gate_decide
from app.core.notifier_bot import get_notifier

# Portfolio-level risk guard (daily limits + per-trade caps)
from app.core import portfolio_guard, feature_store

# ---------- Config & paths ---------- #

ROOT = Path(__file__).resolve().parents[2]

EXEC_ENABLED = str(os.getenv("EXEC_ENABLED", "true")).strip().lower() in ("1", "true", "yes", "on")
EXEC_DRY_RUN = str(os.getenv("EXEC_DRY_RUN", "true")).strip().lower() in ("1", "true", "yes", "on")
EXEC_SIGNALS_PATH = Path(os.getenv("EXEC_SIGNALS_PATH", "signals/observed.jsonl"))
EXEC_POLL_SECONDS = int(os.getenv("EXEC_POLL_SECONDS", "2"))
EXEC_DEFAULT_RISK_PCT = Decimal(os.getenv("EXEC_DEFAULT_RISK_PCT", "0.25"))

# Hard cap for LIVE_CANARY mode (USDT notional per trade)
EXEC_CANARY_MAX_NOTIONAL = Decimal(os.getenv("EXEC_CANARY_MAX_NOTIONAL", "5"))

# Optional: restrict executor to only some sub_uids (comma-separated)
_raw_filter = os.getenv("EXEC_SUB_FILTER", "").strip()
if _raw_filter:
    EXEC_SUB_FILTER: Optional[List[str]] = [s.strip() for s in _raw_filter.split(",") if s.strip()]
else:
    EXEC_SUB_FILTER = None

CATEGORY = "linear"
QUOTE = "USDT"

STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "auto_executor.json"

tg = get_notifier("main")


# ---------- Console + Telegram logging wrappers ---------- #

def log_info(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.info(msg)
    except Exception:
        pass


def log_warn(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.warn(msg)
    except Exception:
        pass


def log_error(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.error(msg)
    except Exception:
        pass


# ---------- State helpers ---------- #

def _load_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.exists():
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"last_offset": 0}


def _save_state(st: Dict[str, Any]) -> None:
    STATE_PATH.write_text(
        json.dumps(st, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )


# ---------- File reading ---------- #

def _read_new_signals(signals_path: Path, last_offset: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    Read any new JSONL signals added after last_offset.
    Returns (signals, new_offset).
    """
    if not signals_path.exists():
        return [], last_offset

    signals: List[Dict[str, Any]] = []
    with signals_path.open("r", encoding="utf-8") as f:
        try:
            f.seek(last_offset)
        except Exception:
            f.seek(0)
            last_offset = 0

        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                sig = json.loads(line)
                if isinstance(sig, dict):
                    signals.append(sig)
            except Exception:
                continue

        new_offset = f.tell()

    return signals, new_offset


# ---------- Helpers ---------- #

def _fmt_dec(x: Decimal) -> str:
    return f"{x.quantize(Decimal('0.0000'), rounding=ROUND_DOWN)}"


def _fmt_usd(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


def _get_open_positions_for_symbol(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch open positions and filter for the given symbol.
    Currently, this uses MAIN-level positions only (no per-sub split yet).
    """
    try:
        pos = list_open_positions()
    except Exception:
        return []
    return [
        p for p in pos
        if p.get("symbol") == symbol and Decimal(str(p.get("size", "0"))) > 0
    ]


def _compute_qty(
    symbol: str,
    risk_pct: Decimal,
    signal: Dict[str, Any],
    automation_mode: str,
) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    Compute position size & risk inputs for Portfolio Guard.

    Steps:
        - get equity in USDT
        - risk_notional = risk_pct% of equity (this is treated as risk_usd)
        - for LIVE_CANARY: cap notional at EXEC_CANARY_MAX_NOTIONAL
        - qty = notional / price

    Returns:
        (qty, equity_usd, risk_usd)

    If anything fails, all three may be None or qty <= 0.
    """
    try:
        equity = Decimal(str(get_equity_usdt()))
    except Exception:
        return None, None, None

    if equity <= 0:
        return None, equity, None

    # Base "risk" notional: conservative approximation of per-trade risk
    risk_usd = (equity * risk_pct / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if risk_usd <= 0:
        return None, equity, risk_usd

    notional = risk_usd

    mode_up = automation_mode.upper()
    if mode_up == "LIVE_CANARY":
        if notional > EXEC_CANARY_MAX_NOTIONAL:
            notional = EXEC_CANARY_MAX_NOTIONAL
            # Risk is now capped to the canary notional
            risk_usd = notional

    px_hint = signal.get("entry_price") or signal.get("price")
    price: Optional[Decimal]
    if px_hint is not None:
        try:
            price = Decimal(str(px_hint))
        except Exception:
            price = None
    else:
        price = None

    if price is None or price <= 0:
        try:
            price = Decimal(str(last_price(symbol)))
        except Exception:
            return None, equity, risk_usd

    if price <= 0:
        return None, equity, risk_usd

    qty = (notional / price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if qty <= 0:
        return None, equity, risk_usd

    return qty, equity, risk_usd


def _place_market_order(symbol: str, side: str, qty: Decimal, reason: str, strategy_lbl: str) -> bool:
    """
    Place a simple market order on Bybit.
    Uses MAIN unified linear perps, CROSS margin, default leverage.
    """
    if qty <= 0:
        log_warn(f"[EXEC] Refusing to place zero qty order for {symbol}.")
        return False

    if EXEC_DRY_RUN:
        log_info(
            f"[EXEC][DRY] {strategy_lbl} → MARKET {symbol} {side} "
            f"qty={_fmt_dec(qty)} | reason={reason}"
        )
        return True

    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,              # "Buy"/"Sell"
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "ImmediateOrCancel",
        "orderLinkId": f"FB_EXEC_{int(time.time() * 1000)}",
    }

    try:
        bybit_post("/v5/order/create", body)
        log_info(
            f"[EXEC] {strategy_lbl} placed MARKET {symbol} {side} "
            f"qty={_fmt_dec(qty)} | reason={reason}"
        )
        return True
    except Exception as e:
        log_error(
            f"[EXEC] {strategy_lbl} order create failed for {symbol} {side} "
            f"qty={_fmt_dec(qty)}: {e}"
        )
        return False


# ---------- Strategy matching ---------- #

def _matching_strategies_for_signal(symbol: str, timeframe: Optional[str]) -> List[Dict[str, Any]]:
    """
    Decide which strategies should consider this signal.

    If timeframe is provided, use strategy_gate.get_strategies_for_signal()
    (symbol + timeframe must both match).

    If timeframe is missing, fall back to "any strategy that trades this symbol".
    """
    if timeframe:
        return get_strategies_for_signal(symbol, timeframe)

    # Fallback: filter by symbol only
    sym_u = symbol.upper().strip()
    matches: List[Dict[str, Any]] = []
    for s in all_strategies():
        if sym_u in s.get("symbols_norm", []):
            matches.append(s)
    return matches


def _sub_filter_allows(sub_uid: str) -> bool:
    if EXEC_SUB_FILTER is None:
        return True
    return str(sub_uid) in EXEC_SUB_FILTER


# ---------- Core per-signal processing ---------- #

def _handle_signal_for_strategy(sig: Dict[str, Any], strat: Dict[str, Any]) -> None:
    symbol = str(sig.get("symbol", "")).strip().upper()
    if not symbol:
        return

    side = sig.get("side") or sig.get("direction")
    if side not in ("Buy", "Sell"):
        return

    sub_uid = str(strat.get("sub_uid_str") or strat.get("sub_uid") or "").strip()
    if not sub_uid:
        return

    if not _sub_filter_allows(sub_uid):
        return

    # Strategy must be enabled and not OFF
    if not is_strategy_enabled(strat):
        return
    if is_strategy_off(strat):
        log_info(f"[EXEC] {strategy_label(strat)} is OFF; ignoring signal for {symbol}.")
        return

    # Concurrency (rough, global per symbol)
    max_conc = strategy_max_concurrent(strat)
    open_pos = _get_open_positions_for_symbol(symbol)
    if max_conc <= 0 and open_pos:
        log_info(
            f"[EXEC] {strategy_label(strat)} has max_concurrent_positions=0 "
            f"and there is already an open {symbol} position; skipping."
        )
        return

    if open_pos:
        log_info(
            f"[EXEC] Global skip for {symbol}: there is already an open position; "
            f"{strategy_label(strat)} will not add another."
        )
        return

    # --- Automation mode handling ----------------
    raw_mode = str(strat.get("automation_mode", "") or "").upper()
    if not raw_mode:
        raw_mode = "LIVE" if is_strategy_live(strat) else "PAPER"

    automation_mode = raw_mode  # e.g. LEARN_DRY, LIVE_CANARY, LIVE_FULL, PAPER

    effective_live = not (EXEC_DRY_RUN or is_strategy_paper(strat))
    effective_mode = automation_mode if effective_live else "PAPER"

    lbl = strategy_label(strat)

    # --- AI gate -------------------
    now = datetime.utcnow()
    hour = now.hour

    est_rr = sig.get("est_rr")
    try:
        est_rr_val = float(est_rr) if est_rr is not None else 0.2
    except Exception:
        est_rr_val = 0.2

    features = {
        "sub_trade_count": int(sig.get("sub_trade_count", 0)),  # TODO: wire from DB later
        "est_rr": est_rr_val,
        "hour_of_day": hour,
    }

    ai_profile = strat.get("ai_profile", "legacy_v1")

    allowed, score, reason_ai = ai_gate_decide(
        sub_uid=sub_uid,
        strategy_id=str(ai_profile),
        features=features,
    )

    if not allowed:
        log_info(
            f"[EXEC][AI_BLOCK] {lbl} symbol={symbol} side={side} "
            f"score={score:.2f} reason={reason_ai} mode={automation_mode}"
        )
        return

    log_info(
        f"[EXEC][AI_OK] {lbl} symbol={symbol} side={side} "
        f"score={score:.2f} reason={reason_ai} "
        f"mode={automation_mode} effective_mode={effective_mode} dry_run={EXEC_DRY_RUN}"
    )

    # --- Risk & sizing -------------
    rpct = Decimal(str(strategy_risk_pct(strat) or EXEC_DEFAULT_RISK_PCT))
    if rpct <= 0:
        log_info(f"[EXEC] {lbl} has 0 risk_per_trade_pct; skipping {symbol}.")
        return

    qty, equity, risk_usd = _compute_qty(symbol, rpct, sig, automation_mode)
    if qty is None or qty <= 0:
        log_warn(
            f"[EXEC] {lbl} unable to compute size for {symbol} "
            f"with risk_pct={rpct} mode={automation_mode}."
        )
        return

    if equity is None or equity <= 0 or risk_usd is None or risk_usd <= 0:
        log_warn(
            f"[EXEC] {lbl} missing equity/risk inputs for {symbol} "
            f"(equity={equity}, risk_usd={risk_usd}); skipping."
        )
        return

    # --- Portfolio Guard check -------------
    guard_allowed, guard_reason = portfolio_guard.can_open_trade(
        sub_uid=sub_uid,
        strategy_name=lbl,
        risk_usd=risk_usd,
        equity_now_usd=equity,
    )

    if not guard_allowed:
        log_warn(
            f"[EXEC][GUARD_BLOCK] {lbl} symbol={symbol} side={side} "
            f"risk={_fmt_usd(risk_usd)} equity={_fmt_usd(equity)} | {guard_reason}"
        )
        return

    reason_str = str(sig.get("reason", "signal")).strip() or "signal"

    # --- Feature Store snapshot -------------
    try:
        feature_store.log_trade_open(
            sub_uid=sub_uid,
            strategy=lbl,
            strategy_id=str(ai_profile),
            symbol=symbol,
            side=side,
            mode=effective_mode,
            equity_usd=equity,
            risk_usd=risk_usd,
            risk_pct=rpct,
            ai_score=float(score),
            ai_reason=str(reason_ai),
            features=features,
            signal=sig,
        )
    except Exception as e:
        # Logging failure shouldn't block trading
        log_warn(f"[EXEC][FEATURE_STORE_FAIL] {lbl} {symbol} | {e}")

    log_info(
        f"[EXEC] {lbl} mode={effective_mode} symbol={symbol} side={side} "
        f"risk_pct={rpct}% risk_usd={_fmt_usd(risk_usd)} qty={_fmt_dec(qty)} "
        f"reason={reason_str}"
    )

    # PAPER / LEARN_DRY / EXEC_DRY_RUN → no live orders
    if effective_mode == "PAPER":
        return

    # Actually place order
    _place_market_order(symbol, side, qty, reason_str, lbl)


def _handle_signal(sig: Dict[str, Any]) -> None:
    symbol = str(sig.get("symbol", "")).strip().upper()
    if not symbol:
        return

    timeframe = sig.get("timeframe") or sig.get("tf") or None
    if isinstance(timeframe, (int, float)):
        timeframe = str(int(timeframe))
    elif isinstance(timeframe, str):
        timeframe = timeframe.strip() or None

    matching_strats = _matching_strategies_for_signal(symbol, timeframe)
    if not matching_strats:
        return

    for strat in matching_strats:
        try:
            _handle_signal_for_strategy(sig, strat)
        except Exception as e:
            log_error(f"[EXEC] Error while handling signal for {strategy_label(strat)}: {e}")


# ---------- Main loop ---------- #

def loop() -> None:
    if not EXEC_ENABLED:
        log_info("⚠️ Auto Executor is disabled (EXEC_ENABLED=false). Exiting.")
        return

    log_info(
        "🔧 Flashback Auto Executor v2 started.\n"
        f"Signals path: {EXEC_SIGNALS_PATH}\n"
        f"State file: {STATE_PATH}\n"
        f"DRY_RUN (global override): {EXEC_DRY_RUN}\n"
        f"Canary max notional: {EXEC_CANARY_MAX_NOTIONAL}\n"
        f"Sub filter: {EXEC_SUB_FILTER if EXEC_SUB_FILTER else 'ALL'}"
    )

    st = _load_state()
    last_offset = int(st.get("last_offset", 0))

    while True:
        try:
            signals, new_offset = _read_new_signals(EXEC_SIGNALS_PATH, last_offset)
            if signals:
                for sig in signals:
                    _handle_signal(sig)
                last_offset = new_offset
                st["last_offset"] = last_offset
                _save_state(st)

            time.sleep(EXEC_POLL_SECONDS)
        except Exception as e:
            log_error(f"[EXEC] Unhandled error in loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    loop()
