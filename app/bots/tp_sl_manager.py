# app/bots/tp_sl_manager.py
# Flashback — TP/SL Manager
# - Watches open positions (linear USDT)
# - For each new or changed position:
#     • sets CROSS + leverage (handled in common helpers if needed elsewhere)
#     • attaches stop-loss via trading-stop
#     • places 5 reduce-only TP limit orders with dynamic spacing
# - Rebalances TP quantities if position size changes
# - Cancels/rebuilds TPs if user cancels them manually

from decimal import Decimal
import time
from typing import Dict, Tuple, List

from app.core.flashback_common import (
    bybit_get, bybit_post, send_tg, list_open_positions,
    get_ticks, psnap, qdown, last_price, atr14,
    set_stop_loss, cancel_all, place_reduce_tp
)

# Pull spacing params from common module constants if present
try:
    from app.core.flashback_common import ATR_MULT, TP5_MAX_ATR_MULT, TP5_MAX_PCT, R_MIN_TICKS
except Exception:
    ATR_MULT = Decimal("1.0")
    TP5_MAX_ATR_MULT = Decimal("3.0")
    TP5_MAX_PCT = Decimal("6.0")
    R_MIN_TICKS = 3

POLL_SECONDS = 3
CATEGORY = "linear"
QUOTE = "USDT"

def _open_orders(symbol: str) -> List[dict]:
    r = bybit_get("/v5/order/realtime", {"category": CATEGORY, "symbol": symbol})
    return r.get("result", {}).get("list", []) or []

def _tp_orders(orders: List[dict], side_now: str) -> List[dict]:
    # TP are reduce-only limits on opposite side
    opp = "Sell" if side_now.lower() == "buy" else "Buy"
    return [
        o for o in orders
        if o.get("orderType") == "Limit"
        and o.get("side") == opp
        and str(o.get("reduceOnly", "False")).lower() == "true"
        and o.get("orderStatus") in ("New", "PartiallyFilled")
    ]

def _compute_exit_grid(symbol: str, side_now: str, entry: Decimal) -> Tuple[Decimal, List[Decimal]]:
    """
    Returns (stop_loss_price, [tp1..tp5]) snapped to valid tick.
    Spacing logic:
      - Base distance = max(ATR * ATR_MULT, R_MIN_TICKS * tick, entry * TP5_MAX_PCT/100 / 5 for tp5 cap)
      - tp_i = entry ± i*R depending on side
      - tp5 capped by TP5_MAX_ATR_MULT * ATR and TP5_MAX_PCT of entry
    """
    tick, _step, _min_notional = get_ticks(symbol)
    atr = atr14(symbol, interval="60")  # 1h ATR; stable but responsive. Adjust if needed.
    if atr <= 0:
        # Fallback if ATR unavailable: 1% band for tp5, so R is 0.2% of entry
        atr = entry * Decimal("0.002")

    # Base R distance from ATR
    R = atr * Decimal(ATR_MULT)

    # Enforce minimum ticks to keep prices valid and not ultra tight
    min_R = tick * Decimal(R_MIN_TICKS)
    if R < min_R:
        R = min_R

    # Cap tp5 by ATR multiple and absolute pct of entry
    max_tp5_dist_atr = atr * Decimal(TP5_MAX_ATR_MULT)
    max_tp5_dist_pct = entry * (Decimal(TP5_MAX_PCT) / Decimal(100))
    max_tp5_dist = min(max_tp5_dist_atr, max_tp5_dist_pct)

    # Stop loss at 1R in opposite direction to entry
    if side_now.lower() == "buy":
        sl = entry - R
        tps = [entry + i * R for i in range(1, 6)]
        if (tps[-1] - entry) > max_tp5_dist:
            # compress R so that tp5 hits the cap
            R = max_tp5_dist / Decimal(5)
            tps = [entry + i * R for i in range(1, 6)]
    else:
        sl = entry + R
        tps = [entry - i * R for i in range(1, 6)]
        if (entry - tps[-1]) > max_tp5_dist:
            R = max_tp5_dist / Decimal(5)
            tps = [entry - i * R for i in range(1, 6)]

    # Snap to tick
    sl = psnap(sl, tick)
    tps = [psnap(px, tick) for px in tps]
    return sl, tps

def _rebalance(symbol: str, side_now: str, size: Decimal, tps: List[Decimal]) -> None:
    """
    Cancels all existing reduce-only TPs and replaces with equal-sized 5-limit ladder.
    """
    tick, step, _ = get_ticks(symbol)
    each = qdown(size / Decimal(5), step)
    # Cancel existing orders for cleanliness
    cancel_all(symbol)
    # Place SL separately via trading-stop is handled by caller
    for px in tps:
        place_reduce_tp(symbol, side_now, each, px)

def _ensure_exits_for_position(p: dict) -> None:
    """
    For a single position record, ensure SL + 5TP exist and are balanced with size.
    """
    symbol = p["symbol"]
    side_now = p["side"]  # "Buy"/"Sell"
    entry = Decimal(p["avgPrice"])
    size = Decimal(p["size"])

    # 1) Compute grid
    sl, tps = _compute_exit_grid(symbol, side_now, entry)

    # 2) Attach/update SL
    set_stop_loss(symbol, sl)

    # 3) Check open orders and decide if we need to rebalance
    oo = _open_orders(symbol)
    tpo = _tp_orders(oo, side_now)
    need_rebuild = False

    if len(tpo) != 5:
        need_rebuild = True
    else:
        # verify prices roughly match our targets; if not, rebuild
        have_prices = sorted([Decimal(o["price"]) for o in tpo])
        want_prices = sorted(tps)
        # allow tiny deviation of one tick
        tick, _step, _ = get_ticks(symbol)
        for hp, wp in zip(have_prices, want_prices):
            if abs(hp - wp) > tick:
                need_rebuild = True
                break
        # verify quantities are equal splits
        if not need_rebuild:
            qtys = [Decimal(o["qty"]) for o in tpo]
            if max(qtys) != min(qtys):
                need_rebuild = True
            else:
                # if equal but not matching current size/5, also rebuild
                expected_each = qdown(size / Decimal(5), _step)
                if qtys[0] != expected_each:
                    need_rebuild = True

    if need_rebuild:
        _rebalance(symbol, side_now, size, tps)

    # 4) Notify once per position attach
    send_tg(f"🎯 Exits set {symbol} {side_now} | SL {sl} | TPs {', '.join(map(str, tps))}")

def loop():
    send_tg("🎛 Flashback TP/SL Manager started.")
    # Track state by (symbol) -> (avgPrice, size)
    seen: Dict[str, Tuple[Decimal, Decimal]] = {}
    while True:
        try:
            positions = list_open_positions()
            current_symbols = set()
            for p in positions:
                symbol = p["symbol"]
                current_symbols.add(symbol)
                state = (Decimal(p["avgPrice"]), Decimal(p["size"]))
                if symbol not in seen or seen[symbol] != state:
                    _ensure_exits_for_position(p)
                    seen[symbol] = state

            # prune symbols no longer open
            for s in list(seen.keys()):
                if s not in current_symbols:
                    seen.pop(s, None)

            time.sleep(POLL_SECONDS)
        except Exception as e:
            send_tg(f"[TP/SL] {e}")
            time.sleep(5)

if __name__ == "__main__":
    loop()
