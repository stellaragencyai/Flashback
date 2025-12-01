# app/bots/drip_speculator.py
# Flashback — Drip Speculator (High-risk sub bot, v1)
#
# For a single subaccount:
# - Watches equity for "drip-style" deposit bumps.
# - After first deposit (and subsequent ones), scans for "perfect" entries on:
#       PUMPFUNUSDT, HBARUSDT, FARTUSDT
#   using:
#       - MA crossover on low timeframe (fast above/below slow),
#       - MA confirmation on higher timeframe,
#       - simple candlestick confirmation.
# - When conditions hit and not paused:
#       • Opens LONG or SHORT using 30% of current equity.
#       • Places 7 TP limit orders (laddered) + SL.
# - Sends Telegram via a dedicated channel:
#       a) on deposit detection
#       b) on trade entry
# - Tracks loss streak:
#       • If 3 losing trades in a row -> pause trading for 24h.
#
# Run one instance per subaccount, configured via env:
#   DRIP_SUB_UID          - Bybit MemberId for this sub
#   DRIP_SUB_LABEL        - label used in logs/TG
#   DRIP_SPEC_SYMBOLS     - comma list, default: "PUMPFUNUSDT,HBARUSDT,FARTUSDT"
#   DRIP_SPEC_TF_FAST     - low timeframe, e.g. "1" (1m)
#   DRIP_SPEC_TF_SLOW     - higher timeframe, e.g. "15" (15m)
#   DRIP_SPEC_NOTIONAL_FRAC - fraction of equity to use (default 0.30)
#   DRIP_SPEC_EQ_DELTA_MIN  - min equity bump to treat as deposit (default 1 USDT)
#   DRIP_SPEC_POLL_SECONDS  - main loop sleep, default 10
#
# Telegram:
#   Uses get_notifier("drip_spec") so you can wire a dedicated bot/chat in notifier config.
#
# State file:
#   state/drip_spec_{DRIP_SUB_LABEL or uid}.json
#     - last_equity
#     - last_deposit_ts
#     - last_closed_row_id
#     - loss_streak
#     - pause_until_ts
#     - last_entry_side
#     - last_entry_symbol
#
# Assumes:
#   - flashback_common.bybit_get / bybit_post target the correct subaccount
#     via that process's API keys (you run this with that sub's keys).
#   - Your TP/SL manager is *not* also trying to manage these same orders.

import os
import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import orjson
from datetime import datetime, timedelta

from app.core.flashback_common import (
    bybit_get,
    bybit_post,
    get_equity_usdt,
)
from app.core.notifier_bot import get_notifier

# ------------ Env & config ------------ #

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SUB_UID = os.getenv("DRIP_SUB_UID", "").strip()
SUB_LABEL = os.getenv("DRIP_SUB_LABEL", SUB_UID or "sub-unknown").strip()

SYMBOLS_RAW = os.getenv(
    "DRIP_SPEC_SYMBOLS",
    "PUMPFUNUSDT,HBARUSDT,FARTUSDT",
)
SYMBOL_WHITELIST = [s.strip().upper() for s in SYMBOLS_RAW.split(",") if s.strip()]

TF_FAST = os.getenv("DRIP_SPEC_TF_FAST", "1")   # 1m
TF_SLOW = os.getenv("DRIP_SPEC_TF_SLOW", "15")  # 15m

NOTIONAL_FRAC = Decimal(os.getenv("DRIP_SPEC_NOTIONAL_FRAC", "0.30"))
EQ_DELTA_MIN = Decimal(os.getenv("DRIP_SPEC_EQ_DELTA_MIN", "1"))
POLL_SECONDS = int(os.getenv("DRIP_SPEC_POLL_SECONDS", "10"))

LOSS_STREAK_LIMIT = 3
PAUSE_HOURS = 24

STATE_PATH = STATE_DIR / f"drip_spec_{SUB_LABEL}.json"

tg = get_notifier("drip_spec")


def log_info(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.info(f"[{SUB_LABEL}] {msg}")
    except Exception:
        pass


def log_warn(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.warn(f"[{SUB_LABEL}] {msg}")
    except Exception:
        pass


def log_error(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.error(f"[{SUB_LABEL}] {msg}")
    except Exception:
        pass


# ------------ State helpers ------------ #


def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {
        "last_equity": None,
        "last_deposit_ts": None,
        "last_closed_row_id": None,
        "loss_streak": 0,
        "pause_until_ts": None,
        "last_entry_side": None,
        "last_entry_symbol": None,
    }


def _save_state(st: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(st))


def _fmt_usd(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


# ------------ Kline / TA helpers ------------ #


def _get_klines(symbol: str, interval: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch OHLCV candles for symbol & interval.
    Uses /v5/market/kline.
    """
    r = bybit_get(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": str(limit),
        },
    )
    lst = r.get("result", {}).get("list", []) or []
    # Bybit returns newest first; we prefer oldest->newest
    return list(reversed(lst))


def _extract_closes(klines: List[Dict[str, Any]]) -> List[Decimal]:
    closes: List[Decimal] = []
    for k in klines:
        # list format: [startTime, open, high, low, close, volume, ...]
        # or dict, depending on wrapper. Handle both.
        if isinstance(k, dict):
            c = k.get("close", None)
        else:
            # assume list
            try:
                c = k[4]
            except Exception:
                c = None
        if c is None:
            continue
        try:
            closes.append(Decimal(str(c)))
        except Exception:
            continue
    return closes


def _sma(values: List[Decimal], window: int) -> Optional[Decimal]:
    if len(values) < window:
        return None
    return sum(values[-window:]) / Decimal(window)


def _ma_pair(values: List[Decimal], fast: int, slow: int) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    return _sma(values, fast), _sma(values, slow)


def _bullish_candle(open_: Decimal, close: Decimal, low: Decimal, high: Decimal) -> bool:
    body = abs(close - open_)
    range_ = high - low if high > low else Decimal("0")
    if range_ <= 0:
        return False
    # simple: close above open, body at least 30% of range
    return close > open_ and body >= range_ * Decimal("0.3")


def _bearish_candle(open_: Decimal, close: Decimal, low: Decimal, high: Decimal) -> bool:
    body = abs(close - open_)
    range_ = high - low if high > low else Decimal("0")
    if range_ <= 0:
        return False
    return close < open_ and body >= range_ * Decimal("0.3")


def _parse_ohlc(latest) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
    """
    Handle dict or list candle -> (open, high, low, close) Decimals.
    """
    if isinstance(latest, dict):
        o = Decimal(str(latest.get("open", "0")))
        h = Decimal(str(latest.get("high", "0")))
        l = Decimal(str(latest.get("low", "0")))
        c = Decimal(str(latest.get("close", "0")))
    else:
        # assume list format
        o = Decimal(str(latest[1]))
        h = Decimal(str(latest[2]))
        l = Decimal(str(latest[3]))
        c = Decimal(str(latest[4]))
    return o, h, l, c


def _entry_signal(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Compute entry signal for symbol.

    LONG:
      - On fast TF:
          fast MA crosses above slow MA on latest candle.
      - On slow TF:
          fast MA > slow MA (uptrend confirmation).
      - Latest candle on fast TF is bullish.
    SHORT:
      - fast TF: fast MA crosses below slow MA.
      - slow TF: fast MA < slow MA (downtrend).
      - Latest candle on fast TF is bearish.
    """
    try:
        k_fast = _get_klines(symbol, TF_FAST, limit=50)
        k_slow = _get_klines(symbol, TF_SLOW, limit=50)
        if len(k_fast) < 10 or len(k_slow) < 10:
            return None

        closes_fast = _extract_closes(k_fast)
        closes_slow = _extract_closes(k_slow)
        if len(closes_fast) < 10 or len(closes_slow) < 10:
            return None

        # Use 5/20 MAs as example
        f_prev, s_prev = _ma_pair(closes_fast[:-1], 5, 20)
        f_now, s_now = _ma_pair(closes_fast, 5, 20)
        f_slow, s_slow = _ma_pair(closes_slow, 5, 20)

        if any(x is None for x in [f_prev, s_prev, f_now, s_now, f_slow, s_slow]):
            return None

        f_prev = f_prev  # type: ignore
        s_prev = s_prev  # type: ignore
        f_now = f_now    # type: ignore
        s_now = s_now    # type: ignore
        f_slow = f_slow  # type: ignore
        s_slow = s_slow  # type: ignore

        latest_fast = k_fast[-1]
        o, h, l, c = _parse_ohlc(latest_fast)

        # LONG candidate
        long_cross = f_prev <= s_prev and f_now > s_now
        long_trend = f_slow > s_slow
        bull = _bullish_candle(o, c, l, h)

        if long_cross and long_trend and bull:
            return {
                "side": "Buy",
                "dir": "LONG",
                "price_hint": c,
            }

        # SHORT candidate
        short_cross = f_prev >= s_prev and f_now < s_now
        short_trend = f_slow < s_slow
        bear = _bearish_candle(o, c, l, h)

        if short_cross and short_trend and bear:
            return {
                "side": "Sell",
                "dir": "SHORT",
                "price_hint": c,
            }

        return None

    except Exception as e:
        log_warn(f"[Signal] {symbol} failed: {e}")
        return None


# ------------ Closed PnL / loss streak ------------ #


def _latest_closed(limit: int = 10) -> List[Dict[str, Any]]:
    r = bybit_get(
        "/v5/position/closed-pnl",
        {"category": "linear", "limit": str(limit)},
    )
    return r.get("result", {}).get("list", []) or []


def _row_id(row: Dict[str, Any]) -> str:
    sym = row.get("symbol", "?")
    ts = row.get("execTime", row.get("updatedTime", "0"))
    return f"{sym}:{ts}"


def _update_loss_streak(st: dict) -> None:
    """
    Look at new closed-pnl rows since last_closed_row_id.
    For each row in our symbol whitelist:
        pnl > 0 -> reset streak = 0
        pnl < 0 -> streak += 1
    """
    last_id = st.get("last_closed_row_id")
    rows = _latest_closed(limit=20)
    if not rows:
        return

    # process from oldest -> newest
    rows_rev = list(reversed(rows))
    new_rows: List[Dict[str, Any]] = []
    for row in rows_rev:
        rid = _row_id(row)
        if last_id is not None and rid == last_id:
            new_rows = []
            continue
        new_rows.append(row)

    loss_streak = int(st.get("loss_streak", 0))

    for row in new_rows:
        rid = _row_id(row)
        sym = str(row.get("symbol", "")).upper()
        if sym not in SYMBOL_WHITELIST:
            last_id = rid
            continue

        pnl = Decimal(str(row.get("closedPnl", "0")))
        if pnl > 0:
            loss_streak = 0
        elif pnl < 0:
            loss_streak += 1

        last_id = rid

    st["last_closed_row_id"] = last_id
    st["loss_streak"] = loss_streak


# ------------ Order helpers ------------ #


def _qty_from_notional(notional: Decimal, price: Decimal) -> str:
    if price <= 0:
        return "0"
    qty = (notional / price).quantize(Decimal("0.000"), rounding=ROUND_DOWN)
    if qty <= 0:
        return "0"
    return str(qty)


def _place_entry_and_tps(
    symbol: str,
    side: str,
    direction: str,
    entry_price: Decimal,
    equity: Decimal,
) -> Optional[Dict[str, Any]]:
    """
    Place a market entry using NOTIONAL_FRAC of equity, then 7 TP limit orders + SL.
    """
    notional = (equity * NOTIONAL_FRAC).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if notional <= 0:
        log_warn("Notional <= 0, skipping entry.")
        return None

    qty_str = _qty_from_notional(notional, entry_price)
    if Decimal(qty_str) <= 0:
        log_warn("Computed qty <= 0, skipping entry.")
        return None

    log_info(
        f"Entering {direction} {symbol} with {qty_str} "
        f"(@ {_fmt_usd(entry_price)} notional {_fmt_usd(notional)})."
    )

    # Entry order: market
    body_entry = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": qty_str,
        "timeInForce": "ImmediateOrCancel",
        "reduceOnly": False,
        "positionIdx": 0,
        "orderLinkId": f"drip_spec_entry_{int(time.time() * 1000)}",
    }

    r = bybit_post("/v5/order/create", body_entry)
    result = r.get("result", {})
    order_id = result.get("orderId")

    if not order_id:
        log_warn("Entry order did not return orderId; TP/SL placement may be off.")
    log_info(f"Entry order placed for {symbol}: {result}")

    # TPs: 7 rungs
    # Use +1.5%, 3%, 4.5%, 6%, 8%, 10%, 12% for LONG
    # and symmetric for SHORT.
    base_steps = [1.5, 3, 4.5, 6, 8, 10, 12]  # percent
    tps: List[Tuple[Decimal, str]] = []

    side_tp = "Sell" if side == "Buy" else "Buy"

    for pct in base_steps:
        step = Decimal(str(pct)) / Decimal("100")
        if direction == "LONG":
            tp_price = (entry_price * (Decimal("1") + step)).quantize(
                Decimal("0.0001"),
                rounding=ROUND_DOWN,
            )
        else:
            tp_price = (entry_price * (Decimal("1") - step)).quantize(
                Decimal("0.0001"),
                rounding=ROUND_DOWN,
            )
        tps.append((tp_price, side_tp))

    # Split qty into 7 equal(ish) chunks
    total_qty = Decimal(qty_str)
    rung_qty = (total_qty / Decimal(len(tps))).quantize(
        Decimal("0.000"),
        rounding=ROUND_DOWN,
    )
    if rung_qty <= 0:
        log_warn("Rung qty <= 0, skipping TP ladder.")
    else:
        for i, (tp_price, tp_side) in enumerate(tps, start=1):
            tp_body = {
                "category": "linear",
                "symbol": symbol,
                "side": tp_side,
                "orderType": "Limit",
                "qty": str(rung_qty),
                "price": str(tp_price),
                "timeInForce": "GoodTillCancel",
                "reduceOnly": True,
                "positionIdx": 0,
                "orderLinkId": f"drip_spec_tp_{i}_{int(time.time() * 1000)}",
            }
            try:
                r_tp = bybit_post("/v5/order/create", tp_body)
                log_info(f"TP{i} placed @ {tp_price} for {symbol}: {r_tp.get('result')}")
            except Exception as e:
                log_warn(f"Failed to place TP{i} for {symbol}: {e}")

    # Stop-loss: about 4% away from entry in adverse direction (high risk)
    sl_step = Decimal("0.04")
    if direction == "LONG":
        sl_price = (entry_price * (Decimal("1") - sl_step)).quantize(
            Decimal("0.0001"),
            rounding=ROUND_DOWN,
        )
        sl_side = "Sell"
    else:
        sl_price = (entry_price * (Decimal("1") + sl_step)).quantize(
            Decimal("0.0001"),
            rounding=ROUND_DOWN,
        )
        sl_side = "Buy"

    sl_body = {
        "category": "linear",
        "symbol": symbol,
        "side": sl_side,
        "orderType": "Stop",
        "qty": qty_str,
        "triggerPrice": str(sl_price),
        "triggerDirection": 2,  # 2 = trigger when lastPrice <= triggerPrice for LONG SL; ok as generic
        "timeInForce": "GoodTillCancel",
        "reduceOnly": True,
        "positionIdx": 0,
        "orderLinkId": f"drip_spec_sl_{int(time.time() * 1000)}",
    }

    try:
        r_sl = bybit_post("/v5/order/create", sl_body)
        log_info(f"SL placed @ {sl_price} for {symbol}: {r_sl.get('result')}")
    except Exception as e:
        log_warn(f"Failed to place SL for {symbol}: {e}")

    tg.info(
        f"[{SUB_LABEL}] 🚀 Entered {direction} on {symbol}\n"
        f"• Entry notional: {_fmt_usd(notional)} ({NOTIONAL_FRAC * 100}% of equity)\n"
        f"• Entry price (hint): {_fmt_usd(entry_price)}\n"
        f"• 7 TP ladder + SL placed."
    )

    return {
        "order_id": order_id,
        "notional": str(notional),
        "qty": qty_str,
    }


# ------------ Equity / deposit detection ------------ #


def _detect_deposit(st: dict, equity: Decimal) -> bool:
    """
    Heuristic: treat any equity bump >= EQ_DELTA_MIN as a "deposit",
    compared to last_equity.

    This will also fire on big PnL jumps, but given these are drip-only
    funding subs, it's a reasonable proxy.
    """
    last_eq_raw = st.get("last_equity")
    if last_eq_raw is None:
        st["last_equity"] = str(equity)
        return False

    last_eq = Decimal(str(last_eq_raw))
    delta = equity - last_eq
    if delta >= EQ_DELTA_MIN:
        st["last_equity"] = str(equity)
        st["last_deposit_ts"] = int(time.time())
        log_info(
            f"💰 Deposit detected: "
            f"equity from {_fmt_usd(last_eq)} → {_fmt_usd(equity)} "
            f"(+{_fmt_usd(delta)})."
        )
        tg.info(
            f"[{SUB_LABEL}] 💰 Drip/Deposit detected.\n"
            f"Equity: {_fmt_usd(equity)} "
            f"(+{_fmt_usd(delta)} vs last)."
        )
        return True

    # small change; just update equity
    st["last_equity"] = str(equity)
    return False


# ------------ Main loop ------------ #


def loop() -> None:
    st = _load_state()
    log_info(
        "🎲 Drip Speculator started.\n"
        f"  SUB_UID: {SUB_UID}\n"
        f"  SYMBOLS: {', '.join(SYMBOL_WHITELIST)}\n"
        f"  TF_FAST: {TF_FAST}m, TF_SLOW: {TF_SLOW}m\n"
        f"  NOTIONAL_FRAC: {NOTIONAL_FRAC}\n"
        f"  EQ_DELTA_MIN: {_fmt_usd(EQ_DELTA_MIN)}\n"
        f"  POLL_SECONDS: {POLL_SECONDS}\n"
        f"  LOSS_STREAK_LIMIT: {LOSS_STREAK_LIMIT}, PAUSE_HOURS: {PAUSE_HOURS}"
    )

    while True:
        try:
            # Update loss streak & pause state from closed PnL
            _update_loss_streak(st)
            loss_streak = int(st.get("loss_streak", 0))

            # Pause logic
            now_ts = int(time.time())
            pause_until_ts = st.get("pause_until_ts")
            if pause_until_ts is not None and now_ts < int(pause_until_ts):
                remaining = int(pause_until_ts) - now_ts
                log_info(
                    f"⏸ Trading paused due to loss streak ({loss_streak}). "
                    f"{remaining // 3600}h{(remaining % 3600) // 60}m left."
                )
                _save_state(st)
                time.sleep(POLL_SECONDS)
                continue

            if loss_streak >= LOSS_STREAK_LIMIT:
                # set pause for 24 hours
                st["pause_until_ts"] = now_ts + PAUSE_HOURS * 3600
                log_warn(
                    f"⛔ Loss streak {loss_streak} reached. "
                    f"Pausing trading for {PAUSE_HOURS}h."
                )
                _save_state(st)
                time.sleep(POLL_SECONDS)
                continue

            # Equity check & deposit detection
            eq = Decimal(str(get_equity_usdt()))
            deposit_fired = _detect_deposit(st, eq)

            # We treat "deposit detected" as a green light to look for entries,
            # but we also allow entries afterwards as long as we're not paused.
            # Now scan for a signal on whitelisted symbols.
            signal: Optional[Dict[str, Any]] = None
            symbol_used: Optional[str] = None

            for sym in SYMBOL_WHITELIST:
                sig = _entry_signal(sym)
                if sig:
                    signal = sig
                    symbol_used = sym
                    break

            if signal and symbol_used:
                side = signal["side"]
                direction = signal["dir"]
                price_hint = signal["price_hint"]  # Decimal

                # Place entry+TP+SL using 30% of current equity
                _place_entry_and_tps(
                    symbol_used,
                    side=side,
                    direction=direction,
                    entry_price=price_hint,
                    equity=eq,
                )

                st["last_entry_side"] = direction
                st["last_entry_symbol"] = symbol_used
                _save_state(st)

            else:
                if deposit_fired:
                    log_info(
                        "Deposit detected but no valid entry signal yet; "
                        "will keep scanning."
                    )
                time.sleep(POLL_SECONDS)
                continue

            _save_state(st)
            time.sleep(POLL_SECONDS)

        except Exception as e:
            log_error(f"[Loop] {e}")
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    loop()
