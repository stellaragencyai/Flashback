#!/usr/bin/env python3
# app/bots/tier_enforcer.py
# Flashback — Tier Enforcer (Main, Simplified Alerts + Hardening)
#
# What it does
# - Detects your current LEVEL (1..9) and maps to TIER (1..3).
# - Notifies when you level up.
# - Enforces tier rules on PENDING entries only:
#     • Max concurrent open positions (Tier 1/2/3 -> 1/2/3)
#     • Per-position notional cap (% of equity, per tier)
# - Cancels newest pending if you're already at concurrency cap.
# - Cancels oversized pending orders and tells you the max legal qty right now.
#
# Telegram messages are short and plain-English.
#
# Enhancements
# - Always passes settleCoin=USDT (avoids retCode 10001)
# - Simple, human alerts with exact “do this” instructions
# - Symbol-specific cooldown to avoid spam
# - Notional cap floor (min $5) so small equity doesn’t produce silly caps
# - Backoff on API hiccups; cool-down after repeated failures

import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import orjson

from app.core.flashback_common import (
    send_tg, get_equity_usdt, list_open_positions, last_price,
    bybit_get, bybit_post, get_ticks, qdown,
    tier_from_equity, cap_pct_for_tier, max_conc_for_tier, TIER_LEVELS,
    MMR_TRIM_TRIGGER
)

POLL_SECONDS = 3
CATEGORY = "linear"
STATE_PATH = Path("app/state/tier_state.json")

# Alert controls
GLOBAL_NOTIFY_COOLDOWN = 180          # seconds between generic “loop active” nudges
SYMBOL_NOTIFY_COOLDOWN = 120          # per-symbol cooldown for oversize/concurrency alerts
MAX_API_ERRORS = 5                    # backoff threshold
MIN_NOTIONAL_FLOOR = Decimal("5")     # don’t set per-position cap below this

def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    # track: last_level, last_global_msg_ts, per-symbol alert ts, rolling error count
    return {"last_level": None, "last_global_msg_ts": 0.0, "symbol_ping": {}, "error_count": 0}

def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(state))

def _level_from_equity(eq: Decimal) -> int:
    level = 1
    for i, th in enumerate(TIER_LEVELS, start=1):
        level = i
        if eq < th:
            break
    return min(level, len(TIER_LEVELS))

def _open_orders_all() -> List[dict]:
    """Fetch all active orders for linear USDT perps."""
    # Prefer /list; fall back to /realtime if needed
    try:
        r = bybit_get("/v5/order/list", {"category": CATEGORY, "settleCoin": "USDT"})
    except Exception:
        r = bybit_get("/v5/order/realtime", {"category": CATEGORY, "settleCoin": "USDT"})
    return r.get("result", {}).get("list", []) or []

def _is_pending_entry(o: dict) -> bool:
    if o.get("orderStatus") not in ("New", "PartiallyFilled"):
        return False
    if str(o.get("reduceOnly", "false")).lower() == "true":
        return False
    if o.get("orderType") not in ("Market", "Limit"):
        return False
    return True

def _pending_entries(orders: List[dict]) -> List[dict]:
    return [o for o in orders if _is_pending_entry(o)]

def _qty_from_order(o: dict) -> Decimal:
    try:
        return Decimal(str(o.get("qty", "0")))
    except Exception:
        return Decimal("0")

def _price_from_order(o: dict) -> Optional[Decimal]:
    try:
        if o.get("orderType") == "Limit":
            return Decimal(str(o.get("price", "0") or "0"))
    except Exception:
        pass
    return None

def _notional(symbol: str, qty: Decimal, px: Optional[Decimal]=None) -> Decimal:
    price = px if px and px > 0 else last_price(symbol)
    if price <= 0:
        return Decimal("0")
    return price * qty

def _cancel_order(order_id: str, symbol: str) -> None:
    try:
        bybit_post("/v5/order/cancel", {"category": CATEGORY, "symbol": symbol, "orderId": order_id})
    except Exception:
        # harmless; next loop will clean up
        pass

def _newest_pending(orders: List[dict]) -> Optional[dict]:
    if not orders:
        return None
    def _ts(o: dict) -> int:
        for k in ("updatedTime", "createdTime", "updatedTimeNs", "createdTimeNs"):
            if k in o and o[k]:
                try:
                    return int(str(o[k])[:13])
                except Exception:
                    pass
        return 0
    return sorted(orders, key=_ts, reverse=True)[0]

def _suggest_qty(symbol: str, equity: Decimal, cap_pct: Decimal) -> Decimal:
    px = last_price(symbol)
    if px <= 0:
        return Decimal("0")
    notional_cap = equity * cap_pct / Decimal(100)
    if notional_cap < MIN_NOTIONAL_FLOOR:
        notional_cap = MIN_NOTIONAL_FLOOR
    _, step, _ = get_ticks(symbol)
    return qdown(notional_cap / px, step)

def _fmt_usd(x: Decimal) -> str:
    try:
        return f"${x:.2f}"
    except Exception:
        return f"${x}"

def _can_ping_symbol(state: dict, symbol: str, now: float) -> bool:
    last = (state.get("symbol_ping") or {}).get(symbol, 0.0)
    return (now - float(last)) >= SYMBOL_NOTIFY_COOLDOWN

def _mark_ping_symbol(state: dict, symbol: str, now: float) -> None:
    d = state.get("symbol_ping") or {}
    d[symbol] = now
    state["symbol_ping"] = d
    _save_state(state)

def _enforce_on_pending(eq: Decimal, tier: int, cap_pct: Decimal, max_conc: int, state: dict) -> None:
    """Enforce concurrency and size caps on pending entries, with simple alerts."""
    now = time.time()
    positions = list_open_positions()
    open_count = len(positions)

    # 1) Concurrency cap: if at cap already, cancel newest pending and explain simply
    if open_count >= max_conc:
        pend = _pending_entries(_open_orders_all())
        if pend:
            newest = _newest_pending(pend)
            if newest:
                sym = newest.get("symbol", "UNKNOWN")
                if _can_ping_symbol(state, sym, now):
                    _cancel_order(newest.get("orderId", ""), sym)
                    send_tg(
                        f"⛔ Too many open trades for Tier {tier} (limit {max_conc}).\n"
                        f"• I cancelled the newest pending order on {sym}.\n"
                        f"• Rule: Tier {tier} allows up to {max_conc} open positions."
                    )
                    _mark_ping_symbol(state, sym, now)

    # 2) Per-position notional cap: cancel oversized pending and show exact max qty
    pend = _pending_entries(_open_orders_all())
    if pend:
        raw_cap = eq * cap_pct / Decimal(100)
        notional_cap = raw_cap if raw_cap >= MIN_NOTIONAL_FLOOR else MIN_NOTIONAL_FLOOR
        for o in pend:
            sym = o.get("symbol", "UNKNOWN")
            qty = _qty_from_order(o)
            px  = _price_from_order(o)
            notional = _notional(sym, qty, px)
            if qty > 0 and notional > notional_cap:
                if _can_ping_symbol(state, sym, now):
                    _cancel_order(o.get("orderId", ""), sym)
                    sugg = _suggest_qty(sym, eq, cap_pct)
                    send_tg(
                        f"📏 {sym} order is too large for Tier {tier}.\n"
                        f"• Your pending size: {_fmt_usd(notional)}\n"
                        f"• Max allowed now: {_fmt_usd(notional_cap)} "
                        f"(Tier {tier} cap {cap_pct}% of equity)\n"
                        f"• Do this: set qty ≤ {sugg}\n"
                        f"Note: I only auto-trim positions if MMR ≥ {MMR_TRIM_TRIGGER}%."
                    )
                    _mark_ping_symbol(state, sym, now)

def loop():
    state = _load_state()
    send_tg("🧭 Tier Enforcer ON. I’ll block oversize entries and extra positions per your tier rules.")
    err_count = 0

    while True:
        try:
            eq = get_equity_usdt()
            tier, level_idx = tier_from_equity(eq)
            cap_pct = cap_pct_for_tier(tier)
            max_conc = max_conc_for_tier(tier)
            now = time.time()

            # Level-up notice
            last_level = state.get("last_level")
            if last_level is None or int(level_idx) > int(last_level):
                state["last_level"] = int(level_idx)
                _save_state(state)
                send_tg(
                    f"🎉 Level {level_idx} reached → Tier {tier}.\n"
                    f"• Max open trades: {max_conc}\n"
                    f"• Per-trade cap: {cap_pct}% of equity (min {_fmt_usd(MIN_NOTIONAL_FLOOR)})"
                )

            # occasional heartbeat (non-spammy)
            if now - float(state.get("last_global_msg_ts", 0.0)) >= GLOBAL_NOTIFY_COOLDOWN:
                state["last_global_msg_ts"] = now
                _save_state(state)
                send_tg(
                    f"🔒 Tier {tier} rules active: up to {max_conc} open trades, "
                    f"{cap_pct}% cap per trade."
                )

            # Enforcement
            _enforce_on_pending(eq, tier, cap_pct, max_conc, state)

            err_count = 0
            time.sleep(POLL_SECONDS)

        except Exception as e:
            err_count += 1
            send_tg(f"[TierEnforcer] {e}")
            if err_count >= MAX_API_ERRORS:
                send_tg("⚠️ Too many API errors. Cooling off 30s.")
                err_count = 0
                time.sleep(30)
            else:
                time.sleep(5)

if __name__ == "__main__":
    loop()
