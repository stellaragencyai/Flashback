# app/bots/risk_guardian.py
# Flashback — Risk Guardian
# Enforces portfolio-tier rules and MMR protection on the MAIN account.
#
# Guarantees:
# - Tier rules:
#     • Tier 1 (levels 1–3): max 1 concurrent position; size cap 30% of equity
#       If >1 positions are open, the NEWEST position is closed immediately (market, reduce-only).
#     • Tier 2 (levels 4–6): max 2 concurrent positions; size cap 22.5%
#     • Tier 3 (levels 7–9): max 3 concurrent positions; size cap 15%
# - MMR safety:
#     • If Maint. Margin Ratio >= 75%: auto trims each open position by 33% (reduce-only, market).
# - Oversize positions (exceeding size cap) below MMR trigger:
#     • Do NOT auto-trim. Only warn on Telegram with the suggested reduce amount.
#
# Notes:
# - Assumes CROSS margin and "max leverage per symbol" are handled elsewhere (entries).
# - Uses Bybit v5 endpoints via flashback_common helpers.

from decimal import Decimal
from typing import List, Dict, Tuple
import time

from app.core.flashback_common import (
    send_tg,
    list_open_positions,
    get_equity_usdt,
    get_mmr_pct,
    tier_from_equity,
    cap_pct_for_tier,
    max_conc_for_tier,
    get_ticks,
    qdown,
    reduce_only_market,
    last_price,
    bybit_get,
)

POLL_SECONDS = 3
MMR_HARD_TRIGGER = Decimal("75")  # percent

def _pos_key(p: dict) -> Tuple[str, str, Decimal]:
    # Unique-ish identity for a position row
    return (p.get("symbol", ""), p.get("side", ""), Decimal(p.get("size", "0")))

def _notional(symbol: str, size: Decimal) -> Decimal:
    px = last_price(symbol)
    return (px * size) if px > 0 else Decimal("0")

def _newest_position(positions: List[dict]) -> dict:
    """
    Pick the newest position by createdTime/updatedTime if present,
    else fall back to the smallest notional (assumed newest add-on).
    """
    # Prefer explicit timestamps if the API returns them
    def _ts(p: dict) -> int:
        # createdTime or updatedTime may appear as strings (ms epoch)
        for k in ("createdTime", "updatedTime", "createdTimeNs", "updatedTimeNs"):
            if k in p and p[k] not in (None, ""):
                try:
                    return int(str(p[k])[:13])  # normalize to ms
                except Exception:
                    pass
        return 0

    positions_sorted = sorted(positions, key=lambda p: _ts(p), reverse=True)
    if positions_sorted and _ts(positions_sorted[0]) > 0:
        return positions_sorted[0]

    # Fallback: smallest notional assumed "newest" risk (conservative close)
    if positions:
        with_notionals = []
        for p in positions:
            try:
                n = _notional(p["symbol"], Decimal(p["size"]))
            except Exception:
                n = Decimal("0")
            with_notionals.append((n, p))
        with_notionals.sort(key=lambda x: x[0])  # smallest first
        return with_notionals[0][1]
    return {}

def _trim_one_third(p: dict) -> str:
    """
    Reduce-only market close for ~33% of the position.
    """
    symbol = p["symbol"]
    side_now = p["side"]  # "Buy"/"Sell"
    size = Decimal(p["size"])
    if size <= 0:
        return f"{symbol} skipped (zero size)."

    # Compute 33% and snap to qty step
    _, step, _ = get_ticks(symbol)
    qty = qdown(size * Decimal("0.33"), step)
    if qty <= 0:
        return f"{symbol} skipped (33% < qty step)."

    try:
        reduce_only_market(symbol, side_now, qty)
        return f"{symbol} trimmed by {qty} (≈33%)."
    except Exception as e:
        return f"{symbol} trim failed: {e}"

def _close_position(p: dict) -> str:
    """
    Close the NEWEST violating position entirely (Tier 1 multi-position rule).
    """
    symbol = p["symbol"]
    side_now = p["side"]
    size = Decimal(p["size"])
    if size <= 0:
        return f"{symbol} close skipped (zero size)."
    _, step, _ = get_ticks(symbol)
    qty = qdown(size, step)
    if qty <= 0:
        return f"{symbol} close skipped (qty < step)."
    try:
        reduce_only_market(symbol, side_now, qty)
        return f"{symbol} newest position closed (Tier 1 single-position rule)."
    except Exception as e:
        return f"{symbol} close failed: {e}"

def _positions_by_symbol(positions: List[dict]) -> Dict[str, List[dict]]:
    m: Dict[str, List[dict]] = {}
    for p in positions:
        m.setdefault(p["symbol"], []).append(p)
    return m

def _enforce_tier_rules(positions: List[dict], tier: int, cap_pct: Decimal, max_conc: int) -> None:
    """
    Enforce:
      - concurrency cap per tier
      - oversize warnings (no auto-trim unless MMR trigger)
      - Tier 1 single-position rule: if >1, close the newest immediately
    """
    # Concurrency across ALL symbols
    total_open = len(positions)
    if tier == 1 and total_open > 1:
        # Close newest position until only 1 remains
        to_close = _newest_position(positions)
        msg = _close_position(to_close)
        send_tg(f"⚠️ Tier 1 violation: >1 positions open. {msg}")
        return  # allow next loop to re-check

    if total_open > max_conc:
        send_tg(f"⚠️ Tier {tier} violation: {total_open}/{max_conc} positions open. Manually close extras.")

    # Oversize warnings per position
    eq = get_equity_usdt()
    if eq <= 0:
        return
    cap_notional = (eq * cap_pct / Decimal(100))

    for p in positions:
        symbol = p["symbol"]
        size = Decimal(p["size"])
        notional = _notional(symbol, size)
        if notional > cap_notional:
            # suggest a reduce qty that brings it down to cap
            over = notional - cap_notional
            _, step, _ = get_ticks(symbol)
            px = last_price(symbol)
            if px > 0:
                suggest_qty = qdown(over / px, step)
                send_tg(
                    f"🔶 Oversize alert: {symbol} notional ${notional:.2f} exceeds Tier {tier} cap "
                    f"(${cap_notional:.2f}). Suggest reduce ≈ {suggest_qty} to comply. "
                    f"(No auto-trim below MMR trigger.)"
                )

def _mmr_guard(positions: List[dict]) -> None:
    """
    If MMR >= trigger, auto-trim all open positions by ~33% to de-risk immediately.
    """
    mmr = get_mmr_pct()
    if mmr < MMR_HARD_TRIGGER:
        return
    results = []
    for p in positions:
        results.append(_trim_one_third(p))
    joined = " | ".join(results) if results else "no positions"
    send_tg(f"🧯 MMR {mmr:.2f}% >= {MMR_HARD_TRIGGER}% → auto trims: {joined}")

def loop():
    send_tg("🛡️ Flashback Risk Guardian started.")
    while True:
        try:
            positions = list_open_positions()

            # MMR first: if we are over the line, act immediately
            _mmr_guard(positions)

            # Tier evaluation
            eq = get_equity_usdt()
            tier, level_idx = tier_from_equity(eq)
            cap_pct = cap_pct_for_tier(tier)
            max_conc = max_conc_for_tier(tier)

            _enforce_tier_rules(positions, tier, cap_pct, max_conc)

            time.sleep(POLL_SECONDS)
        except Exception as e:
            send_tg(f"[RiskGuardian] {e}")
            time.sleep(5)

if __name__ == "__main__":
    loop()
