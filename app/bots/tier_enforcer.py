#!/usr/bin/env python3
# app/bots/tier_enforcer.py
# Flashback — Tier Enforcer (Main, Enhanced)
#
# Purpose:
#   - Tracks level (1–9) and tier (1–3) based on account equity.
#   - Notifies on milestone level-ups.
#   - Enforces tier-based rules on pending entries:
#       * Max concurrent positions
#       * Max per-position notional cap (% of equity)
#   - Cancels any new or oversized pending orders beyond limits.
#   - Suggests valid qtys for oversized entries.
#
# Enhancements:
#   ✅ Fixed missing settleCoin (retCode 10001)
#   ✅ Added auto error backoff and resilience
#   ✅ Added rate limit for repeated Telegram alerts
#   ✅ Improved Bybit API handling and fallback safety
#   ✅ Default safeguard for missing symbols
#
# Dependencies:
#   - app/core/flashback_common.py
#   - .env must have TIER_LEVELS, cap %, TG_TOKEN_MAIN, TG_CHAT_MAIN

import time
import math
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import orjson

from app.core.flashback_common import (
    send_tg, get_equity_usdt, list_open_positions, last_price,
    bybit_get, bybit_post, get_ticks, qdown,
    tier_from_equity, cap_pct_for_tier, max_conc_for_tier, TIER_LEVELS
)

POLL_SECONDS = 3
CATEGORY = "linear"
STATE_PATH = Path("app/state/tier_state.json")

# Rate-limit settings (seconds)
NOTIFY_COOLDOWN = 180  # minimum interval before re-sending same tier message
MAX_API_ERRORS = 5     # breaker threshold

def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {"last_level": None, "last_tier_msg": 0, "error_count": 0}

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
    """Fetch all active orders (fallback-safe)."""
    try:
        r = bybit_get("/v5/order/list", {"category": CATEGORY, "settleCoin": "USDT"})
    except Exception:
        # fallback in case /list misbehaves
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

def _notional(symbol: str, qty: Decimal, px: Optional[Decimal]=None) -> Decimal:
    price = px if px and px > 0 else last_price(symbol)
    if price <= 0:
        return Decimal("0")
    return price * qty

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

def _cancel_order(order_id: str, symbol: str) -> None:
    try:
        bybit_post("/v5/order/cancel", {"category": CATEGORY, "symbol": symbol, "orderId": order_id})
    except Exception:
        pass  # next loop will catch remaining

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
    _, step, _ = get_ticks(symbol)
    return qdown(notional_cap / px, step)

def _enforce_on_pending(eq: Decimal, tier: int, cap_pct: Decimal, max_conc: int) -> None:
    """Core logic enforcing concurrency & size caps on pending entries."""
    positions = list_open_positions()
    open_count = len(positions)

    # 1) Enforce max concurrent open positions
    if open_count >= max_conc:
        pend = _pending_entries(_open_orders_all())
        if pend:
            newest = _newest_pending(pend)
            if newest:
                _cancel_order(newest.get("orderId", ""), newest.get("symbol", ""))
                send_tg(f"⛔ Tier {tier} cap {max_conc} reached. "
                        f"Canceled newest pending entry on {newest.get('symbol', 'unknown')}.")

    # 2) Enforce notional size per order
    pend = _pending_entries(_open_orders_all())
    if pend:
        notional_cap = eq * cap_pct / Decimal(100)
        for o in pend:
            sym = o.get("symbol", "unknown")
            qty = _qty_from_order(o)
            px  = _price_from_order(o)
            notional = _notional(sym, qty, px)
            if notional > notional_cap and qty > 0:
                _cancel_order(o.get("orderId", ""), sym)
                sugg = _suggest_qty(sym, eq, cap_pct)
                send_tg(
                    f"🟨 Oversized entry {sym}: ${notional:.2f} > cap ${notional_cap:.2f}. "
                    f"Canceled. Suggested qty ≈ {sugg}."
                )

def loop():
    state = _load_state()
    send_tg("📈 Flashback Tier Enforcer started.")
    last_tg = state.get("last_tier_msg", 0)
    err_count = 0

    while True:
        try:
            eq = get_equity_usdt()
            tier, level_idx = tier_from_equity(eq)
            cap_pct = cap_pct_for_tier(tier)
            max_conc = max_conc_for_tier(tier)
            now = time.time()

            # Announce current tier once per session or per major level-up
            last_level = state.get("last_level")
            if last_level is None or int(level_idx) > int(last_level):
                state["last_level"] = int(level_idx)
                _save_state(state)
                send_tg(f"🎯 Level {level_idx} reached → Tier {tier} active. "
                        f"Cap {cap_pct}% | Max {max_conc} positions.")

            # Prevent redundant spam if equity fluctuates near thresholds
            if now - last_tg > NOTIFY_COOLDOWN:
                send_tg(f"💡 Tier {tier} enforcement loop active | "
                        f"Cap {cap_pct}% | Max {max_conc} positions.")
                state["last_tier_msg"] = now
                _save_state(state)
                last_tg = now

            _enforce_on_pending(eq, tier, cap_pct, max_conc)
            err_count = 0  # reset on success
            time.sleep(POLL_SECONDS)

        except Exception as e:
            err_count += 1
            send_tg(f"[TierEnforcer] {e}")
            if err_count >= MAX_API_ERRORS:
                send_tg(f"⚠️ TierEnforcer paused ({err_count} API errors). Cooling off 30s.")
                err_count = 0
                time.sleep(30)
            else:
                time.sleep(5)

if __name__ == "__main__":
    loop()
