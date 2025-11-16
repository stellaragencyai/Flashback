#!/usr/bin/env python3
# app/bots/tier_enforcer.py
# Flashback — Tier Enforcer (Main, Plain-English Alerts, low-noise)

import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import os
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

# ----------------- Noise controls & limits -----------------

# Old "NOTIFY_COOLDOWN" is now configurable and defaulted to 1 hour.
TIER_NOTIFY_INTERVAL_SEC = int(os.getenv("TIER_NOTIFY_INTERVAL_SEC", "3600"))
NOTIFY_COOLDOWN = TIER_NOTIFY_INTERVAL_SEC

MAX_API_ERRORS = 5

# Stop yelling about tiny positions (already enforced)
MIN_NOTIONAL_FLOOR = Decimal("5")  # bump to 10 if you want fewer enforced caps

# Extra: only alert about oversize orders if notional >= this
TIER_MIN_ALERT_NOTIONAL_USD = Decimal(os.getenv("TIER_MIN_ALERT_NOTIONAL_USD", "25"))

# Extra: per-symbol cooldown for oversize alerts (seconds)
TIER_OVERSIZE_ALERT_COOLDOWN_SEC = int(os.getenv("TIER_OVERSIZE_ALERT_COOLDOWN_SEC", "300"))

# Extra: cooldown for concurrency alerts (seconds)
TIER_CONCURRENCY_ALERT_COOLDOWN_SEC = int(os.getenv("TIER_CONCURRENCY_ALERT_COOLDOWN_SEC", "600"))

# Extra: cooldown for error alerts (seconds)
TIER_ERROR_ALERT_COOLDOWN_SEC = int(os.getenv("TIER_ERROR_ALERT_COOLDOWN_SEC", "120"))

_last_oversize_alert: Dict[str, float] = {}
_last_concurrency_alert: float = 0.0
_last_error_alert: float = 0.0


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
    # Prefer /list, fall back to /realtime; always pin settleCoin to avoid API 10001
    try:
        r = bybit_get("/v5/order/list", {"category": CATEGORY, "settleCoin": "USDT"})
    except Exception:
        r = bybit_get("/v5/order/realtime", {"category": CATEGORY, "settleCoin": "USDT"})
    return r.get("result", {}).get("list", []) or []


def _is_pending_entry(o: dict) -> bool:
    # Pending entry = New/PartiallyFilled, not reduce-only, Market/Limit
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
            val = o.get("price", "") or "0"
            return Decimal(str(val))
    except Exception:
        pass
    return None


def _notional(symbol: str, qty: Decimal, px: Optional[Decimal] = None) -> Decimal:
    price = px if px and px > 0 else last_price(symbol)
    if price <= 0:
        return Decimal("0")
    return price * qty


def _cancel_order(order_id: str, symbol: str) -> None:
    try:
        bybit_post("/v5/order/cancel", {"category": CATEGORY, "symbol": symbol, "orderId": order_id})
    except Exception:
        pass  # next loop will mop up


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


def _fmt_qty(x: Decimal) -> str:
    # keep natural string for contract qty
    s = str(x.normalize())
    return s if "E" not in s else f"{x:.8f}"


def _should_alert_oversize(symbol: str) -> bool:
    now = time.time()
    last = _last_oversize_alert.get(symbol)
    if last is not None and now - last < TIER_OVERSIZE_ALERT_COOLDOWN_SEC:
        return False
    _last_oversize_alert[symbol] = now
    return True


def _should_alert_concurrency() -> bool:
    global _last_concurrency_alert
    now = time.time()
    if now - _last_concurrency_alert < TIER_CONCURRENCY_ALERT_COOLDOWN_SEC:
        return False
    _last_concurrency_alert = now
    return True


def _should_alert_error() -> bool:
    global _last_error_alert
    now = time.time()
    if now - _last_error_alert < TIER_ERROR_ALERT_COOLDOWN_SEC:
        return False
    _last_error_alert = now
    return True


def _announce_current(eq: Decimal, tier: int, level_idx: int, cap_pct: Decimal, max_conc: int) -> None:
    cap_usd = eq * cap_pct / Decimal(100)
    if cap_usd < MIN_NOTIONAL_FLOOR:
        cap_usd = MIN_NOTIONAL_FLOOR
    send_tg(
        "▶ Tier rules active:\n"
        f"• Level {level_idx} → Tier {tier}\n"
        f"• Max open positions: {max_conc}\n"
        f"• Max size per position: {_fmt_usd(cap_usd)} ({cap_pct}% of equity)\n"
        f"• Auto-trim only if MMR ≥ {MMR_TRIM_TRIGGER}%"
    )


def _enforce_on_pending(eq: Decimal, tier: int, cap_pct: Decimal, max_conc: int) -> None:
    positions = list_open_positions()
    open_count = len(positions)
    now = time.time()

    # 1) Concurrency cap
    if open_count >= max_conc:
        pend = _pending_entries(_open_orders_all())
        if pend:
            newest = _newest_pending(pend)
            if newest:
                sym = newest.get("symbol", "UNKNOWN")
                _cancel_order(newest.get("orderId", ""), sym)
                if _should_alert_concurrency():
                    send_tg(
                        f"❌ Too many positions: you already have {open_count}/{max_conc} open.\n"
                        f"I canceled the newest pending order on {sym}."
                    )

    # 2) Per-position size cap (by notional)
    pend = _pending_entries(_open_orders_all())
    if not pend:
        return

    notional_cap = eq * cap_pct / Decimal(100)
    if notional_cap < MIN_NOTIONAL_FLOOR:
        notional_cap = MIN_NOTIONAL_FLOOR

    for o in pend:
        sym = o.get("symbol", "UNKNOWN")
        qty = _qty_from_order(o)
        px = _price_from_order(o)
        notional = _notional(sym, qty, px)
        if qty <= 0 or notional <= 0:
            continue
        if notional > notional_cap:
            _cancel_order(o.get("orderId", ""), sym)
            sugg = _suggest_qty(sym, eq, cap_pct)

            # Silent for tiny orders; still enforce, just don't ping you.
            if notional < TIER_MIN_ALERT_NOTIONAL_USD:
                continue

            if _should_alert_oversize(sym):
                send_tg(
                    "⚠️ Order too large for your current tier.\n"
                    f"• Symbol: {sym}\n"
                    f"• Your order: {_fmt_usd(notional)}\n"
                    f"• Limit now: {_fmt_usd(notional_cap)}\n"
                    f"• Max allowed qty right now: {_fmt_qty(sugg)}\n"
                    "I canceled that pending order. Re-place with qty at or below the limit."
                )


def loop():
    state = _load_state()
    send_tg("📈 Tier Enforcer is running.")
    last_tg = state.get("last_tier_msg", 0)
    err_count = 0

    # Announce once at start
    try:
        eq0 = get_equity_usdt()
        t0, lvl0 = tier_from_equity(eq0)
        _announce_current(eq0, t0, lvl0, cap_pct_for_tier(t0), max_conc_for_tier(t0))
    except Exception:
        pass

    while True:
        try:
            eq = get_equity_usdt()
            tier, level_idx = tier_from_equity(eq)
            cap_pct = cap_pct_for_tier(tier)
            max_conc = max_conc_for_tier(tier)
            now = time.time()

            # Milestone level-up
            last_level = state.get("last_level")
            if last_level is None or int(level_idx) > int(last_level):
                state["last_level"] = int(level_idx)
                _save_state(state)
                send_tg(f"🎉 Level up to {level_idx}. Tier {tier} rules applied.")
                _announce_current(eq, tier, level_idx, cap_pct, max_conc)

            # Periodic gentle reminder (now limited to once per TIER_NOTIFY_INTERVAL_SEC)
            if now - last_tg > NOTIFY_COOLDOWN:
                cap_usd = eq * cap_pct / Decimal(100)
                if cap_usd < MIN_NOTIONAL_FLOOR:
                    cap_usd = MIN_NOTIONAL_FLOOR
                send_tg(
                    f"🔁 Tier {tier} active | max positions {max_conc} | "
                    f"max size per position {_fmt_usd(cap_usd)}."
                )
                state["last_tier_msg"] = now
                _save_state(state)
                last_tg = now

            _enforce_on_pending(eq, tier, cap_pct, max_conc)
            err_count = 0
            time.sleep(POLL_SECONDS)

        except Exception as e:
            err_count += 1
            if _should_alert_error():
                send_tg(f"[TierEnforcer] {e}")
            if err_count >= MAX_API_ERRORS:
                send_tg("⏸ Too many API errors. Cooling off for 30s.")
                err_count = 0
                time.sleep(30)
            else:
                time.sleep(5)


if __name__ == "__main__":
    loop()
