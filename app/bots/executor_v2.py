#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Auto Executor v2 (Strategy-aware, multi-sub, AI-gated, policy-aware)

Purpose
-------
- Consume signals from an append-only JSONL file (signals/observed.jsonl).
- For EACH strategy defined in config/strategies.yaml:
    • Check symbol + timeframe match via strategy_gate.
    • Check enabled + automation mode.
    • Run AI gating (classifier + min-score policy).
    • Run correlation gate.
    • Size entries (bayesian + risk_capped, policy-adjusted risk).
    • Log feature snapshot for setup memory.
    • Place live or paper entries depending on automation_mode.

Automation modes
----------------
- OFF         : ignore strategy.
- LEARN_DRY   : run AI + logging, NO live orders (paper / learning only).
- LIVE_CANARY : live orders with small risk (as per strategies.yaml + policy).
- LIVE_FULL   : normal live trading (once proven).

Exits:
- TP/SL handled by a separate bot (tp_sl_manager).

Notes
-----
- Stateless across runs except for the cursor file.
- Strategy definitions live in config/strategies.yaml.
"""

from __future__ import annotations

import json
import asyncio
import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, List, Any

from app.core.config import settings

# ---------- Robust logger import ---------- #
try:
    # Preferred: dedicated logger module
    from app.core.logger import get_logger, bind_context  # type: ignore
except Exception:
    try:
        # Fallback: older / alternate logging module
        from app.core.log import get_logger as _get_logger  # type: ignore

        import logging

        def bind_context(logger: "logging.Logger", **ctx):
            """
            Minimal bind_context fallback:
            just returns the same logger, ignoring context.
            """
            return logger

        get_logger = _get_logger  # type: ignore
    except Exception:
        # Last resort: plain stdlib logging
        import logging

        def get_logger(name: str) -> "logging.Logger":  # type: ignore
            logger_ = logging.getLogger(name)
            if not logger_.handlers:
                handler = logging.StreamHandler()
                fmt = logging.Formatter(
                    "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
                )
                handler.setFormatter(fmt)
                logger_.addHandler(handler)
            logger_.setLevel(logging.INFO)
            return logger_

        def bind_context(logger: "logging.Logger", **ctx):  # type: ignore
            return logger

from app.core.bybit_client import Bybit
from app.core.notifier_bot import tg_send
from app.core.feature_store import log_features
from app.core.trade_classifier import classify as classify_trade
from app.core.corr_gate_v2 import allow as corr_allow
from app.core.sizing import bayesian_size, risk_capped_qty
from app.core.strategy_gate import (
    get_strategies_for_signal,
    strategy_label,
    strategy_risk_pct,
)
from app.core.portfolio_guard import can_open_trade
from app.core.flashback_common import get_equity_usdt, record_heartbeat
from app.core.session_guard import should_block_trading
from app.ai.setup_memory_policy import get_risk_multiplier, get_min_ai_score

# NEW: orders bus for AI/journal spine
from app.core.orders_bus import record_order_event


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


# ---------- AI GATE WRAPPER ---------- #

def run_ai_gate(signal: Dict[str, Any], strat_id: str, bound_log) -> Dict[str, Any]:
    """
    Unified wrapper around trade_classifier + policy threshold.

    Returns a dict with at least:
      {
        "allow": bool,
        "score": float | None,
        "reason": str,
        "features": dict
      }
    """
    # 1) Classifier itself
    try:
        clf = classify_trade(signal, strat_id)
    except Exception as e:
        bound_log.warning(
            "AI classifier crashed or misbehaved for [%s]: %r — bypassing gate (allow=True).",
            strat_id,
            e,
        )
        return {
            "allow": True,
            "score": None,
            "reason": f"classifier_error: {e}",
            "features": {},
        }

    if not isinstance(clf, dict):
        bound_log.warning(
            "AI classifier returned non-dict for [%s]: %r — treating as allow=True.",
            strat_id,
            clf,
        )
        return {
            "allow": True,
            "score": None,
            "reason": "classifier_non_dict",
            "features": {},
        }

    allow = bool(clf.get("allow", True))
    score = clf.get("score")
    reason = clf.get("reason") or clf.get("why") or "ok"
    features = clf.get("features") or {}

    # 2) Policy-based min score per strategy
    min_score = get_min_ai_score(strat_id)
    score_f: Optional[float]
    try:
        score_f = float(score) if score is not None else None
    except Exception:
        score_f = None

    if min_score > 0 and score_f is not None and score_f < min_score:
        bound_log.info(
            "AI score %.3f < min threshold %.3f for [%s]; rejecting trade.",
            score_f,
            min_score,
            strat_id,
        )
        return {
            "allow": False,
            "score": score_f,
            "reason": f"score_below_min ({score_f:.3f} < {min_score:.3f})",
            "features": features,
        }

    if not allow:
        bound_log.info("AI gate rejected [%s]: %s", strat_id, reason)

    return {
        "allow": allow,
        "score": score_f,
        "reason": reason,
        "features": features,
    }


# ---------- SIGNAL PROCESSOR ---------- #

async def process_signal_line(line: str) -> None:
    try:
        sig = json.loads(line)
    except json.JSONDecodeError:
        log.warning("Invalid JSON in observed.jsonl: %r", line[:200])
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe") or sig.get("tf")
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

def _automation_mode_from_cfg(cfg: Dict[str, Any]) -> str:
    mode = str(cfg.get("automation_mode", "OFF")).upper().strip()
    if mode not in ("OFF", "LEARN_DRY", "LIVE_CANARY", "LIVE_FULL"):
        mode = "OFF"
    return mode


async def handle_strategy_signal(
    strat_name: str,
    strat_cfg: Dict[str, Any],
    sig: Dict[str, Any],
) -> None:
    strat_id = strategy_label(strat_cfg)  # e.g. "Sub2_Breakout(524633243)"
    bound = bind_context(log, strat=strat_id)

    enabled = bool(strat_cfg.get("enabled", False))
    mode_raw = _automation_mode_from_cfg(strat_cfg)

    if not enabled or mode_raw == "OFF":
        bound.debug("strategy disabled or automation_mode=OFF")
        return

    symbol = sig.get("symbol")
    tf = sig.get("timeframe") or sig.get("tf")
    side = sig.get("side")
    price = sig.get("price") or sig.get("last") or sig.get("close")
    ts = sig.get("ts") or sig.get("timestamp")

    if not symbol or not tf or not side or price is None:
        bound.warning("missing required fields in signal: %r", sig)
        return

    try:
        price_f = float(price)
    except Exception:
        bound.warning("invalid price in signal: %r", sig)
        return

    # Normalize mode for feature logging
    if mode_raw == "LEARN_DRY":
        trade_mode = "PAPER"
    elif mode_raw == "LIVE_CANARY":
        trade_mode = "LIVE_CANARY"
    elif mode_raw == "LIVE_FULL":
        trade_mode = "LIVE_FULL"
    else:
        trade_mode = mode_raw

    # Sub UID for logging + guards
    sub_uid = str(
        strat_cfg.get("sub_uid")
        or strat_cfg.get("subAccountId")
        or strat_cfg.get("accountId")
        or strat_cfg.get("subId")
        or ""
    )

    # Account label for orders_bus / state joins (e.g. "main", "flashback10")
    account_label = str(
        strat_cfg.get("account_label")
        or strat_cfg.get("label")
        or strat_cfg.get("account_label_slug")
        or "main"
    )

    # ---------- Session Guard (loss streak / daily limits) ---------- #
    try:
        if should_block_trading():
            bound.info("Session Guard blocking new trades (limits reached).")
            return
    except Exception as e:
        bound.warning("Session Guard error; bypassing: %r", e)

    # ---------- AI Classifier + Policy Gate ---------- #
    ai = run_ai_gate(sig, strat_id, bound)
    if not ai["allow"]:
        return

    # ---------- Correlation gate (unpack (allowed, reason)) ---------- #
    try:
        allowed_corr, corr_reason = corr_allow(symbol)
    except Exception as e:
        bound.warning("Correlation gate error for %s: %r; bypassing.", symbol, e)
        allowed_corr, corr_reason = True, "corr_gate_v2 exception, bypassed"

    if not allowed_corr:
        bound.info("Correlation gate rejected for %s: %s", symbol, corr_reason)
        return

    # ---------- Sizing + Risk Policy ---------- #
    # Strategy risk (% of equity per trade, or fraction) from YAML
    try:
        base_risk_pct = Decimal(str(strategy_risk_pct(strat_cfg)))
    except Exception:
        base_risk_pct = Decimal("0")

    # Policy multiplier per strategy
    risk_mult = Decimal(str(get_risk_multiplier(strat_id)))
    eff_risk_pct = base_risk_pct * risk_mult

    if eff_risk_pct <= 0:
        bound.info("effective risk_pct <= 0 for %s; skipping.", strat_id)
        return

    # Fetch current equity (MAIN unified)
    try:
        equity_val = Decimal(str(get_equity_usdt()))
    except Exception as e:
        bound.warning("get_equity_usdt failed: %r; assuming equity=0.", e)
        equity_val = Decimal("0")

    # bayesian_size is assumed to return (notional_usd, stop_distance)
    try:
        notional_usd, stop_distance = bayesian_size(
            symbol=symbol,
            side=side,
            price=price_f,
            strategy_id=strat_id,
        )
    except TypeError:
        # Backward-compat: older signature bayesian_size(symbol, side, price)
        notional_usd, stop_distance = bayesian_size(symbol, side, price_f)

    try:
        notional_dec = Decimal(str(notional_usd))
    except Exception:
        notional_dec = Decimal("0")

    if notional_dec <= 0:
        bound.info(
            "notional_usd <= 0 after bayesian_size for %s; equity=%s risk_pct=%s",
            strat_id,
            equity_val,
            eff_risk_pct,
        )
        return

    # Risk-capped quantity, using effective risk %.
    try:
        qty = risk_capped_qty(
            symbol=symbol,
            notional_usd=notional_dec,
            equity_usd=equity_val,
            max_risk_pct=eff_risk_pct,
            stop_distance=stop_distance,
        )
    except TypeError:
        # Backward-compat: older signature risk_capped_qty(symbol, raw_qty)
        qty = risk_capped_qty(symbol, notional_dec)

    try:
        qty_dec = Decimal(str(qty))
    except Exception:
        qty_dec = Decimal("0")

    if qty_dec <= 0:
        bound.info(
            "qty <= 0 after sizing; skipping entry. notional_usd=%s equity=%s risk_pct=%s",
            notional_dec,
            equity_val,
            eff_risk_pct,
        )
        return

    # Approximate risk in USD (for logging / guard)
    try:
        risk_usd = equity_val * eff_risk_pct
    except Exception:
        risk_usd = Decimal("0")

    # ---------- Portfolio Guard (global / per-trade caps) ---------- #
    try:
        guard_ok, guard_reason = can_open_trade(
            sub_uid=sub_uid or None,
            strategy_name=strat_cfg.get("name", strat_name),
            risk_usd=risk_usd,
            equity_now_usd=equity_val,
        )
    except TypeError:
        # older implementations may just return bool / legacy signature
        try:
            guard_ok = bool(can_open_trade(symbol, float(risk_usd)))
            guard_reason = "legacy_bool_guard"
        except Exception as e:
            bound.warning("Portfolio guard failed for %s: %r; bypassing.", symbol, e)
            guard_ok, guard_reason = True, "guard_exception_bypass"
    except Exception as e:
        bound.warning("Portfolio guard failed for %s: %r; bypassing.", symbol, e)
        guard_ok, guard_reason = True, "guard_exception_bypass"

    if not guard_ok:
        bound.info("Portfolio guard blocked trade for %s: %s", symbol, guard_reason)
        return

    # ---------- Feature logging (for setup memory) ---------- #
    try:
        ai_score = ai.get("score")
        ai_reason = ai.get("reason", "")
        features = ai.get("features") or {}

        log_features(
            sub_uid=sub_uid,
            strategy=strat_cfg.get("name", strat_name),
            strategy_id=strat_id,
            symbol=symbol,
            side=side,
            mode=trade_mode,
            equity_usd=equity_val,
            risk_usd=risk_usd,
            risk_pct=eff_risk_pct,
            ai_score=float(ai_score) if ai_score is not None else 0.0,
            ai_reason=str(ai_reason),
            features=features,
            signal=sig,
        )
    except Exception as e:
        bound.warning("feature logging failed: %r", e)

    # ---------- Execute or paper log ---------- #
    if mode_raw in ("LIVE_CANARY", "LIVE_FULL"):
        await execute_entry(
            symbol=symbol,
            signal_side=str(side),
            qty=float(qty_dec),
            price=price_f,
            strat=strat_id,
            mode=trade_mode,
            sub_uid=sub_uid,
            account_label=account_label,
            bound_log=bound,
        )
    else:
        bound.info(
            "PAPER entry [%s]: %s %s qty=%s @ ~%s (risk_mult=%.2f)",
            strat_id,
            symbol,
            side,
            qty_dec,
            price_f,
            float(risk_mult),
        )


# ---------- EXECUTOR ---------- #

def _normalize_order_side(signal_side: str) -> str:
    """
    Normalize signal side into Bybit API side ("Buy"/"Sell").
    Accepts: "buy"/"sell"/"long"/"short" (case-insensitive).
    """
    s = str(signal_side or "").strip().lower()
    if s in ("buy", "long"):
        return "Buy"
    if s in ("sell", "short"):
        return "Sell"
    # If some genius sends nonsense, fail loudly in logs.
    raise ValueError(f"Unsupported side value for order: {signal_side!r}")


async def execute_entry(
    symbol: str,
    signal_side: str,
    qty: float,
    price: float,
    strat: str,
    mode: str,
    sub_uid: str,
    account_label: str,
    bound_log,
) -> None:
    client = get_trade_client()
    try:
        order_side = _normalize_order_side(signal_side)
    except ValueError as e:
        bound_log.error("cannot normalize side %r for %s: %r", signal_side, symbol, e)
        return

    # Unique orderLinkId so journal + setup_memory can join reliably
    order_link_id = f"{strat}-{int(time.time() * 1000)}"

    try:
        resp = client.place_order(
            category="linear",
            symbol=symbol,
            side=order_side,
            qty=qty,
            orderType="Market",
            orderLinkId=order_link_id,
        )
        bound_log.info(
            "LIVE entry executed [%s %s]: %s %s qty=%s resp=%r",
            mode,
            strat,
            symbol,
            order_side,
            qty,
            resp,
        )

        # --- Orders bus logging (NEW event) ---
        try:
            result = resp.get("result") if isinstance(resp, dict) else None
            r = result or {}

            order_id = (
                r.get("orderId")
                or r.get("order_id")
                or r.get("orderID")
                or order_link_id
            )
            status = (
                r.get("orderStatus")
                or r.get("order_status")
                or "New"
            )
            order_type = r.get("orderType") or r.get("order_type") or "Market"

            price_str = str(r.get("price") or price)
            qty_str = str(r.get("qty") or qty)
            cum_exec_qty = str(r.get("cumExecQty") or r.get("cum_exec_qty") or 0)
            cum_exec_value = str(r.get("cumExecValue") or r.get("cum_exec_value") or 0)
            cum_exec_fee = str(r.get("cumExecFee") or r.get("cum_exec_fee") or 0)

            record_order_event(
                account_label=account_label,
                symbol=symbol,
                order_id=str(order_id),
                side=order_side,
                order_type=str(order_type),
                status=str(status),
                event_type="NEW",
                price=price_str,
                qty=qty_str,
                cum_exec_qty=cum_exec_qty,
                cum_exec_value=cum_exec_value,
                cum_exec_fee=cum_exec_fee,
                position_side=None,
                reduce_only=r.get("reduceOnly"),
                client_order_id=order_link_id,
                raw={
                    "api_response": r,
                    "sub_uid": sub_uid,
                    "mode": mode,
                    "strategy": strat,
                },
            )
        except Exception as e:
            bound_log.warning("orders_bus logging failed for %s %s: %r", symbol, order_link_id, e)

        try:
            tg_send(
                f"🚀 Entry placed [{mode}/{strat}] {symbol} {order_side} qty={qty} "
                f"linkId={order_link_id}"
            )
        except Exception as e:
            bound_log.warning("telegram send failed: %r", e)
    except Exception as e:
        bound_log.error(
            "order failed for %s %s qty=%s (strat=%s): %r",
            symbol,
            order_side,
            qty,
            strat,
            e,
        )


# ---------- MAIN LOOP ---------- #

async def executor_loop() -> None:
    pos = load_cursor()
    log.info("executor_v2 starting at cursor=%s", pos)

    while True:
        try:
            # heartbeat for supervisor / liveness tracking
            record_heartbeat("executor_v2")

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
                        log.warning(
                            "executor_v2: failed to decode line at pos=%s: %r",
                            pos,
                            e,
                        )
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
