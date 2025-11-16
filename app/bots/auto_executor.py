# app/bots/auto_executor.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Auto Executor v1

Purpose:
    - Consume signals from an append-only JSONL file (signals/observed.jsonl).
    - For a given logical subaccount (EXEC_SUB_UID), as defined in strategies.yaml:
        • Filter signals by allowed symbols and basic strategy constraints.
        • Run them through the AI gate (ai_gate_decide).
        • If allowed, size and place entry orders on Bybit (MAIN unified account).
    - Let TP/SL Manager handle exits via ladders and trading-stop.

Notes / Assumptions:
    - This v1 executes on the MAIN account using BYBIT_MAIN_* keys from .env.
      EXEC_SUB_UID is treated as the "logical owner" of the strategy for AI and logging.
      Later, we can extend this to use per-sub API keys to truly trade subaccounts.
    - Signals are emitted by your Signal Engine into JSONL format; expected fields:
        {
            "symbol": "BTCUSDT",
            "side": "Buy",          # or "Sell"
            "reason": "trend_breakout",
            "ts_ms": 1731712345678,
            "est_rr": 0.25,         # optional, for AI gate
            "stop_price": 62000.0,  # optional, for sizing
            "entry_hint": "market"  # optional: "market" or "limit"
        }
      Extra fields are ignored.

Env (.env):
    EXEC_SUB_UID=524630315                  # which logical sub this executor represents
    EXEC_DRY_RUN=true|false                 # if true, logs instead of placing real orders
    EXEC_SIGNALS_PATH=signals/observed.jsonl
    EXEC_POLL_SECONDS=2
    EXEC_DEFAULT_RISK_PCT=0.25              # fallback if strategy has no risk_per_trade_pct

Dependencies:
    - app.core.flashback_common:
        bybit_post, get_equity_usdt, list_open_positions, last_price
    - app.core.strategies:
        get_strategy_for_sub
    - app.ai.executor_ai_gate:
        ai_gate_decide
    - app.core.notifier_bot:
        get_notifier("main")

State:
    - state/auto_executor_<sub_uid>.json:
        {
            "last_offset": <int>   # byte offset in signals file; used to resume
        }
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Optional, List

# --- Core imports from your stack ---
from app.core.flashback_common import (
    bybit_post,
    get_equity_usdt,
    list_open_positions,
    last_price,
)

from app.core.strategies import get_strategy_for_sub
from app.ai.executor_ai_gate import ai_gate_decide
from app.core.notifier_bot import get_notifier

# ---------- Config & paths ---------- #

ROOT = Path(__file__).resolve().parents[2]

EXEC_SUB_UID: Optional[str] = os.getenv("EXEC_SUB_UID", "").strip() or None
if EXEC_SUB_UID is None:
    raise RuntimeError("EXEC_SUB_UID is not set in .env; executor doesn't know which sub it's running for.")

EXEC_DRY_RUN = str(os.getenv("EXEC_DRY_RUN", "true")).strip().lower() in ("1", "true", "yes", "on")
EXEC_SIGNALS_PATH = Path(os.getenv("EXEC_SIGNALS_PATH", "signals/observed.jsonl"))
EXEC_POLL_SECONDS = int(os.getenv("EXEC_POLL_SECONDS", "2"))
EXEC_DEFAULT_RISK_PCT = Decimal(os.getenv("EXEC_DEFAULT_RISK_PCT", "0.25"))

CATEGORY = "linear"
QUOTE = "USDT"

STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / f"auto_executor_{EXEC_SUB_UID}.json"

tg = get_notifier("main")


# ---------- State helpers ---------- #

def _load_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.exists():
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"last_offset": 0}


def _save_state(st: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(st, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")


# ---------- Helpers ---------- #

def _fmt_dec(x: Decimal) -> str:
    return f"{x.quantize(Decimal('0.0000'), rounding=ROUND_DOWN)}"


def _fmt_usd(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


def _read_new_signals(signals_path: Path, last_offset: int) -> (List[Dict[str, Any]], int):
    """
    Read any new JSONL signals added after last_offset.
    Returns (signals, new_offset).
    """
    if not signals_path.exists():
        return [], last_offset

    signals: List[Dict[str, Any]] = []
    with signals_path.open("r", encoding="utf-8") as f:
        # Seek to last read position
        try:
            f.seek(last_offset)
        except Exception:
            # If file shrank or something weird, reset to 0
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
                # malformed line; skip
                continue

        new_offset = f.tell()

    return signals, new_offset


def _get_open_positions_for_symbol(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch open positions and filter for the given symbol.
    Currently, this uses MAIN-level positions (no per-sub split yet).
    """
    try:
        pos = list_open_positions()
    except Exception:
        return []
    return [p for p in pos if p.get("symbol") == symbol and Decimal(str(p.get("size", "0"))) > 0]


def _max_concurrent_open_for_sub(strategy_cfg: Dict[str, Any]) -> int:
    try:
        return int(strategy_cfg.get("max_concurrent_positions", 1))
    except Exception:
        return 1


def _allowed_symbols(strategy_cfg: Dict[str, Any]) -> List[str]:
    syms = strategy_cfg.get("symbols", [])
    if not isinstance(syms, list):
        return []
    return [str(s).strip() for s in syms if str(s).strip()]


def _risk_pct_for_sub(strategy_cfg: Dict[str, Any]) -> Decimal:
    try:
        rp = Decimal(str(strategy_cfg.get("risk_per_trade_pct", EXEC_DEFAULT_RISK_PCT)))
    except Exception:
        rp = EXEC_DEFAULT_RISK_PCT
    return rp


def _compute_qty(symbol: str,
                 side: str,
                 risk_pct: Decimal,
                 signal: Dict[str, Any]) -> Optional[Decimal]:
    """
    Compute a naive qty:
        - get equity in USDT
        - risk_pct% of equity as notional
        - qty = notional / price
    We can refine later (true SL-based R, tick/step, etc.)
    """
    try:
        equity = Decimal(str(get_equity_usdt()))
    except Exception:
        return None

    if equity <= 0:
        return None

    notional = (equity * risk_pct / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if notional <= 0:
        return None

    # Use latest price (or a hint from signal if available)
    px_hint = signal.get("entry_price") or signal.get("price")
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
            return None

    if price <= 0:
        return None

    qty = (notional / price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if qty <= 0:
        return None

    return qty


def _place_market_order(symbol: str, side: str, qty: Decimal, reason: str) -> bool:
    """
    Place a simple market order on Bybit.
    Uses MAIN unified linear perps, CROSS margin, default leverage.
    """
    if qty <= 0:
        tg.warn(f"[EXEC] Refusing to place zero qty order for {symbol}.")
        return False

    if EXEC_DRY_RUN:
        tg.info(
            f"[EXEC][DRY] Would place MARKET order: {symbol} {side} qty={_fmt_dec(qty)} | reason={reason}"
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
        tg.info(
            f"[EXEC] Placed MARKET order: {symbol} {side} qty={_fmt_dec(qty)} | reason={reason}"
        )
        return True
    except Exception as e:
        tg.error(f"[EXEC] Order create failed for {symbol} {side} qty={_fmt_dec(qty)}: {e}")
        return False


# ---------- Core processing ---------- #

def _handle_signal(sig: Dict[str, Any], strategy_cfg: Dict[str, Any]) -> None:
    symbol = str(sig.get("symbol", "")).strip()
    side = sig.get("side") or sig.get("direction")  # sometimes "direction" is used
    if not symbol or side not in ("Buy", "Sell"):
        return

    # Optional: signal-level routing; if signal declares a target sub_uid and it isn't ours, skip
    target_sub = sig.get("sub_uid") or sig.get("sub") or None
    if target_sub is not None and str(target_sub) != EXEC_SUB_UID:
        return

    allowed_syms = _allowed_symbols(strategy_cfg)
    if allowed_syms and symbol not in allowed_syms:
        tg.info(
            f"[EXEC] Skip {symbol}: not in allowed symbols for sub {EXEC_SUB_UID} ({allowed_syms})."
        )
        return

    # Enforce max concurrent positions (rough main-level count for now)
    max_conc = _max_concurrent_open_for_sub(strategy_cfg)
    open_pos = _get_open_positions_for_symbol(symbol)
    if open_pos and max_conc <= 0:
        tg.info(
            f"[EXEC] Skip {symbol}: max_concurrent_positions=0 but there is already an open position."
        )
        return

    # For now, we don't track per-sub open count; we just prevent multiple entries
    # on the same symbol concurrently.
    if open_pos:
        tg.info(
            f"[EXEC] Skip {symbol}: already have open position on this symbol."
        )
        return

    # Build features for AI gate
    now = datetime.utcnow()
    hour = now.hour

    est_rr = sig.get("est_rr")
    try:
        est_rr_val = float(est_rr) if est_rr is not None else 0.2
    except Exception:
        est_rr_val = 0.2

    features = {
        "sub_trade_count": int(sig.get("sub_trade_count", 0)),  # TODO: wire to DB later
        "est_rr": est_rr_val,
        "hour_of_day": hour,
    }

    ai_profile = strategy_cfg.get("ai_profile", "legacy_v1")

    allowed, score, reason = ai_gate_decide(
        sub_uid=EXEC_SUB_UID,
        strategy_id=str(ai_profile),
        features=features,
    )

    if not allowed:
        tg.info(
            f"[EXEC][AI_BLOCK] sub={EXEC_SUB_UID} symbol={symbol} side={side} "
            f"score={score:.2f} reason={reason}"
        )
        return

    tg.info(
        f"[EXEC][AI_OK] sub={EXEC_SUB_UID} symbol={symbol} side={side} "
        f"score={score:.2f} reason={reason}"
    )

    # Compute size
    risk_pct = _risk_pct_for_sub(strategy_cfg)
    qty = _compute_qty(symbol, side, risk_pct, sig)
    if qty is None or qty <= 0:
        tg.warn(
            f"[EXEC] Unable to compute position size for {symbol} with risk_pct={risk_pct}."
        )
        return

    # Place order
    reason_str = str(sig.get("reason", "signal")).strip() or "signal"
    _place_market_order(symbol, side, qty, reason_str)


def loop() -> None:
    """
    Main loop:
        - Load strategy config for EXEC_SUB_UID.
        - Watch signals file for new lines.
        - Process each new signal through strategy + AI gate + order placement.
    """
    strategy_cfg = get_strategy_for_sub(EXEC_SUB_UID)
    if not strategy_cfg:
        raise RuntimeError(f"No strategy config found for EXEC_SUB_UID={EXEC_SUB_UID}")

    tg.info(
        f"🔧 Flashback Auto Executor started for sub_uid={EXEC_SUB_UID} "
        f"(role={strategy_cfg.get('role')}, DRY_RUN={EXEC_DRY_RUN}).\n"
        f"Signals path: {EXEC_SIGNALS_PATH}\n"
        f"State file: {STATE_PATH}"
    )

    st = _load_state()
    last_offset = int(st.get("last_offset", 0))

    while True:
        try:
            signals, new_offset = _read_new_signals(EXEC_SIGNALS_PATH, last_offset)
            if signals:
                for sig in signals:
                    _handle_signal(sig, strategy_cfg)
                last_offset = new_offset
                st["last_offset"] = last_offset
                _save_state(st)

            time.sleep(EXEC_POLL_SECONDS)
        except Exception as e:
            tg.error(f"[EXEC] Unhandled error in loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    loop()
