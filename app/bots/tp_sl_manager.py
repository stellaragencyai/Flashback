# app/bots/tp_sl_manager.py
# Flashback — TP/SL Manager v6
#
# Key upgrades (v6):
# - 7-TP ladder support (strategy-aware exit profiles).
# - Strategy-aware exit behavior:
#     • Attempts to look up per-subaccount strategy from config/strategies.yaml.
#     • Uses per-strategy exit_profile when present, otherwise falls back to default.
# - Trailing SL:
#     • Base SL still comes from ATR/R logic.
#     • SL is trailed off best favorable price using R-based distance.
# - SL distance widened:
#     • SL now uses a separate multiplier (SL_R_MULT, default 2.2x) so stops
#       are significantly further from entry than TP spacing.
# - Manual SL override:
#     • If the exchange stop-loss is moved far away from our computed SL,
#       we enter "manual SL mode" for that symbol.
#     • In manual SL mode we DO NOT call set_stop_loss again, so your manual SL
#       is respected until flat.
# - Still:
#     • Uses Bybit V5 PRIVATE WEBSOCKET for near-instant position updates (optional).
#     • NEVER nukes the whole TP ladder on size change (amend-in-place).
#     • Keeps SL attached via trading-stop helper (unless manual override).
#     • ATR caching to avoid hammering indicators.
#
# New behavior (v4/v5 recap + v6):
#   1) "No TP" watchdog:
#      - On every cycle, for every open position, we inspect open orders.
#      - If ALL TP orders have been canceled, we IMMEDIATELY rebuild the TP ladder
#        (7 exits or a single mid TP for tiny positions), regardless of whether
#        avgPrice/size changed.
#
#   2) Manual TP override respect:
#      - If we detect TP prices deviating significantly from our computed grid,
#        we treat that symbol as "manual TP override":
#           • We STOP amending TP prices for that symbol.
#           • We can still rebalance quantities to cover size (optional).
#           • We send a one-time Telegram notice the first time this is seen.
#      - Manual mode resets when:
#           • Position is closed, or
#           • All TPs are canceled and we rebuild from scratch.
#
#   3) Manual SL override (v6):
#      - If there's an existing stopLoss on the position that deviates by more
#        than ~2 ticks from our computed base SL, we assume you manually moved it.
#      - We then enter manual SL mode and DO NOT call set_stop_loss for that
#        symbol anymore until the position is flat.
#
#   4) Exit profiles (strategy-aware):
#      - Each strategy in config/strategies.yaml may define an exit_profile, e.g.:
#           exit_profile:
#             name: standard_7
#             tp_count: 7
#             trailing_sl: true
#      - If absent, we fall back to DEFAULT_EXIT_PROFILE (7 TPs + trailing SL).
#
#   ENV knobs:
#     TPM_USE_WEBSOCKET=true|false
#     TPM_POLL_SECONDS=2          (HTTP mode only; also used as cadence for TP checks)
#     TPM_ATR_CACHE_SEC=60
#     TPM_RESPECT_MANUAL_TPS=true|false   (default true)
#     TPM_TRAIL_R_MULT=1.0        (R-multiple used for trailing distance)
#     TPM_SL_R_MULT=2.2           (R-multiple used ONLY for SL distance from entry)
#
#   ATR_MULT, TP5_MAX_ATR_MULT, TP5_MAX_PCT, R_MIN_TICKS come from flashback_common or defaults.
#   Note: TP5_* names are kept for backward compatibility; they still cap the *furthest* TP
#         even though we now use 7 levels.

import os
import time
import json
import hmac
import hashlib
from decimal import Decimal
from typing import Dict, Tuple, List, Optional

from app.core.flashback_common import (
    bybit_get,
    bybit_post,
    send_tg,
    list_open_positions,
    get_ticks,
    psnap,
    qdown,
    last_price,
    atr14,
    set_stop_loss,
    cancel_all,      # kept for emergencies only; not used in normal flow
    place_reduce_tp,
)

# Optional websocket support (websocket-client)
try:
    import websocket  # type: ignore
except ImportError:
    websocket = None

# Strategy registry (for per-sub exit profiles)
try:
    from app.core import strategies as strat_mod  # expects get_strategy_for_sub(sub_uid)
except Exception:
    strat_mod = None  # type: ignore

# ---- Spacing params from common module if present ----
try:
    from app.core.flashback_common import ATR_MULT, TP5_MAX_ATR_MULT, TP5_MAX_PCT, R_MIN_TICKS
except Exception:
    ATR_MULT = Decimal("1.0")
    TP5_MAX_ATR_MULT = Decimal("3.0")
    TP5_MAX_PCT = Decimal("6.0")
    R_MIN_TICKS = 3

CATEGORY = "linear"
QUOTE = "USDT"

# Core number of rungs for the default ladder.
CORE_TP_COUNT = 7

# Polling cadence (HTTP mode only + safety watchdog)
POLL_SECONDS = int(os.getenv("TPM_POLL_SECONDS", "2"))

# WebSocket toggle & URL
USE_WS = os.getenv("TPM_USE_WEBSOCKET", "false").strip().lower() == "true"
WS_PRIVATE_URL = os.getenv("BYBIT_WS_PRIVATE_URL", "wss://stream.bybit.com/v5/private")

# HMAC creds for private WS auth (use trading key)
WS_KEY = os.getenv("BYBIT_MAIN_TRADE_KEY", "")
WS_SECRET = os.getenv("BYBIT_MAIN_TRADE_SECRET", "")

# Respect manual TP modifications (prices) or not
_RESPECT_MANUAL_TPS = os.getenv("TPM_RESPECT_MANUAL_TPS", "true").strip().lower() == "true"

# Trailing SL config
_TRAIL_R_MULT = Decimal(os.getenv("TPM_TRAIL_R_MULT", "1.0"))

# SL distance multiplier (relative to base R)
SL_R_MULT = Decimal(os.getenv("TPM_SL_R_MULT", "2.2"))

# ATR cache: symbol -> (ts, atr)
_ATR_CACHE_TTL = int(os.getenv("TPM_ATR_CACHE_SEC", "60"))
_ATR_CACHE: Dict[str, Tuple[float, Decimal]] = {}

# Manual TP override per symbol: if True, we do NOT amend TP prices for that symbol.
_MANUAL_TP_MODE: Dict[str, bool] = {}

# Manual SL override per symbol: if True, we do NOT call set_stop_loss for that symbol.
_MANUAL_SL_MODE: Dict[str, bool] = {}

# Trailing SL state per symbol:
#   symbol -> {
#       "entry": Decimal,
#       "base_sl": Decimal,
#       "best": Decimal,
#   }
_TRAIL_STATE: Dict[str, Dict[str, Decimal]] = {}

# Default exit profile (used if strategy lookup fails or is absent)
DEFAULT_EXIT_PROFILE = {
    "name": "standard_7",
    "tp_count": CORE_TP_COUNT,
    "trailing_sl": True,
}


def _open_orders(symbol: str) -> List[dict]:
    r = bybit_get("/v5/order/realtime", {"category": CATEGORY, "symbol": symbol})
    return r.get("result", {}).get("list", []) or []


def _tp_orders(orders: List[dict], side_now: str) -> List[dict]:
    # TP = reduce-only limits on opposite side
    opp = "Sell" if side_now.lower() == "buy" else "Buy"
    return [
        o for o in orders
        if o.get("orderType") == "Limit"
        and o.get("side") == opp
        and str(o.get("reduceOnly", "False")).lower() == "true"
        and o.get("orderStatus") in ("New", "PartiallyFilled")
    ]


def _get_atr(symbol: str, entry: Decimal) -> Decimal:
    """
    Cached ATR(14) on 1h. Fallback to synthetic 0.2% R if missing.
    """
    now = time.time()
    cached = _ATR_CACHE.get(symbol)
    if cached is not None:
        ts, val = cached
        if now - ts < _ATR_CACHE_TTL:
            return val

    atr_val = atr14(symbol, interval="60")
    if atr_val <= 0:
        # Fallback: synthetic ~0.2% band; we'll transform into R later.
        atr_val = entry * Decimal("0.002")

    _ATR_CACHE[symbol] = (now, atr_val)
    return atr_val


def _compute_exit_grid(symbol: str, side_now: str, entry: Decimal) -> Tuple[Decimal, List[Decimal]]:
    """
    Returns (stop_loss_price, [tp1..tpN]) snapped to valid tick.

    Spacing logic:
      - Base R_base = max(ATR * ATR_MULT, R_MIN_TICKS * tick)
      - SL distance uses R_sl = R_base * SL_R_MULT  (wider stop)
      - TP spacing uses R_tp = R_base               (tighter rungs)
      - tpi = entry ± i*R_tp depending on side, for i in [1..CORE_TP_COUNT]
      - furthest TP capped by:
          • TP5_MAX_ATR_MULT * ATR distance   (legacy name, still used as max-multiple)
          • TP5_MAX_PCT% of entry             (legacy name, still used as max % band)
      - If cap is hit, R_tp is compressed so that tpn - entry == max_tp_dist.
    """
    tick, _step, _min_notional = get_ticks(symbol)

    atr = _get_atr(symbol, entry)
    if atr <= 0:
        # Paranoid fallback – use small percent band directly
        atr = entry * Decimal("0.002")

    # Base R distance
    R_base = atr * Decimal(ATR_MULT)

    # Enforce minimum ticks
    min_R = tick * Decimal(R_MIN_TICKS)
    if R_base < min_R:
        R_base = min_R

    # Separate distances for TP vs SL
    R_tp = R_base
    R_sl = R_base * SL_R_MULT

    # Cap furthest TP
    max_tp_dist_atr = atr * Decimal(TP5_MAX_ATR_MULT)
    max_tp_dist_pct = entry * (Decimal(TP5_MAX_PCT) / Decimal(100))
    max_tp_dist = min(max_tp_dist_atr, max_tp_dist_pct)

    if side_now.lower() == "buy":
        sl = entry - R_sl
        tps = [entry + i * R_tp for i in range(1, CORE_TP_COUNT + 1)]
        if (tps[-1] - entry) > max_tp_dist:
            R_tp = max_tp_dist / Decimal(CORE_TP_COUNT)
            tps = [entry + i * R_tp for i in range(1, CORE_TP_COUNT + 1)]
    else:
        sl = entry + R_sl
        tps = [entry - i * R_tp for i in range(1, CORE_TP_COUNT + 1)]
        if (entry - tps[-1]) > max_tp_dist:
            R_tp = max_tp_dist / Decimal(CORE_TP_COUNT)
            tps = [entry - i * R_tp for i in range(1, CORE_TP_COUNT + 1)]

    # Snap to tick
    sl = psnap(sl, tick)
    tps = [psnap(px, tick) for px in tps]
    return sl, tps


def _get_exit_profile_for_position(p: dict) -> Dict[str, object]:
    """
    Determine exit profile for a given position using strategy config when possible.

    Expected strategy config (config/strategies.yaml):
      - Each sub_uid strategy may define:
          exit_profile:
            name: standard_7
            tp_count: 7
            trailing_sl: true
    """
    profile = dict(DEFAULT_EXIT_PROFILE)

    if strat_mod is None:
        return profile

    # sub_uid field may be present depending on how list_open_positions is wired.
    sub_uid = (
        p.get("sub_uid")
        or p.get("subAccountId")
        or p.get("accountId")
        or p.get("subId")
    )
    if not sub_uid:
        return profile

    try:
        strat = strat_mod.get_strategy_for_sub(str(sub_uid))
    except Exception:
        strat = None

    if not strat:
        return profile

    cfg = strat.get("exit_profile") or strat.get("exitProfile")
    if isinstance(cfg, dict):
        # Merge dict into profile
        if "name" in cfg:
            profile["name"] = cfg["name"]
        if "tp_count" in cfg:
            try:
                tp_count_val = int(cfg["tp_count"])
                if tp_count_val > 0:
                    profile["tp_count"] = min(tp_count_val, CORE_TP_COUNT)
            except Exception:
                pass
        if "trailing_sl" in cfg:
            profile["trailing_sl"] = bool(cfg["trailing_sl"])
    elif isinstance(cfg, str):
        # Named profile; for now we only meaningfully support "standard_7".
        name = cfg.strip().lower()
        if name == "standard_7":
            profile["name"] = "standard_7"
            profile["tp_count"] = CORE_TP_COUNT
            profile["trailing_sl"] = True

    return profile


def _compute_trailing_sl(
    symbol: str,
    side_now: str,
    entry: Decimal,
    base_sl: Decimal,
    tps: List[Decimal],
    trailing_enabled: bool,
) -> Decimal:
    """
    Compute a trailing SL based on best favorable price and R distance.

    Behavior:
      - If trailing is disabled, returns base_sl.
      - R is derived from distance between entry and first TP (or entry/base_sl).
      - Trailing distance = R * TPM_TRAIL_R_MULT.
      - For longs:
          best = max(best, last_price)
          sl = max(base_sl, best - trail_dist)
        For shorts:
          best = min(best, last_price)
          sl = min(base_sl, best + trail_dist)
      - SL never "un-trails" (we don't move it away from price).
    """
    if not trailing_enabled or _TRAIL_R_MULT <= 0:
        return base_sl

    try:
        price = Decimal(str(last_price(symbol)))
    except Exception:
        # If we can't get price, just keep base SL.
        return base_sl

    if price <= 0:
        return base_sl

    # Derive R from first TP or base SL if needed
    if tps:
        R = abs(tps[0] - entry)
    else:
        R = abs(entry - base_sl)

    if R <= 0:
        return base_sl

    trail_dist = R * _TRAIL_R_MULT

    state = _TRAIL_STATE.get(symbol)
    if state is None or state.get("entry") != entry or state.get("base_sl") != base_sl:
        # New position or re-anchored; reset trail state.
        state = {
            "entry": entry,
            "base_sl": base_sl,
            "best": price,
        }
    else:
        # Update best favorable price
        best = state.get("best", entry)
        if side_now.lower() == "buy":
            if price > best:
                best = price
        else:
            # For shorts, "best" is the lowest price seen
            if price < best:
                best = price
        state["best"] = best

    best = state["best"]

    if side_now.lower() == "buy":
        sl_candidate = best - trail_dist
        # Never trail below the original base SL
        sl_new = max(base_sl, sl_candidate)
    else:
        sl_candidate = best + trail_dist
        # For shorts, SL is above price; never trail above base SL
        sl_new = min(base_sl, sl_candidate)

    # Snap to tick
    tick, _step, _ = get_ticks(symbol)
    sl_new = psnap(sl_new, tick)

    _TRAIL_STATE[symbol] = state
    return sl_new


def _amend_tp_order(symbol: str, order: dict,
                    new_qty: Optional[Decimal],
                    new_price: Optional[Decimal]) -> None:
    """
    Amend a single TP order in place via REST /v5/order/amend.
    No blanket cancel/rebuild. If API fails, we log but don't crash.
    """
    body: Dict[str, str] = {
        "category": CATEGORY,
        "symbol": symbol,
    }
    order_id = order.get("orderId")
    link_id = order.get("orderLinkId")
    if order_id:
        body["orderId"] = order_id
    elif link_id:
        body["orderLinkId"] = link_id
    else:
        # No identifier? Nothing we can safely amend.
        return

    if new_price is not None:
        body["price"] = str(new_price)
    if new_qty is not None:
        body["qty"] = str(new_qty)

    try:
        bybit_post("/v5/order/amend", body)
    except Exception as e:
        send_tg(f"[TP/SL amend] {symbol} error: {e}")


def _detect_manual_override(symbol: str,
                            tpo: List[dict],
                            target_tps: List[Decimal],
                            core_count: int) -> bool:
    """
    Heuristic: if a majority of TP prices deviate from our ideal grid by more than
    ~2 ticks, assume the user manually moved them and enter manual TP mode.

    This is deliberately forgiving. If you nudge one TP a bit we won't freak out,
    but if you reshape the ladder, we stop fighting you.
    """
    if not tpo or not target_tps:
        return False

    tick, _step, _ = get_ticks(symbol)
    cur_prices = sorted(Decimal(o["price"]) for o in tpo)
    tgt_sorted = sorted(target_tps)

    n = min(len(cur_prices), len(tgt_sorted), core_count)
    if n == 0:
        return False

    mismatches = 0
    for i in range(n):
        if abs(cur_prices[i] - tgt_sorted[i]) > (tick * 2):
            mismatches += 1

    # Two or more TPs significantly off-grid => treating as manual override.
    return mismatches >= 2


def _sync_tp_ladder(symbol: str,
                    side_now: str,
                    size: Decimal,
                    tps: List[Decimal],
                    tp_count: int) -> None:
    """
    Ensure we have a TP ladder (up to CORE_TP_COUNT), **without** ever nuking the book.

    Behavior:
      - If no TPs:
          • place fresh `tp_count` reduce-only orders (or single mid TP if tiny).
      - If some TPs:
          • If TPM_RESPECT_MANUAL_TPS=true and we detect a manual override,
            we DO NOT amend TP prices; we only (optionally) rebalance qty.
          • Otherwise: we amend TPs in place to match target prices / equal qty splits.
      - Extra TPs (from manual tinkering) are left alone; we only manage up to CORE_TP_COUNT
        and only the first `tp_count` as the "core ladder".
    """
    # Clamp tp_count to sane range
    if tp_count <= 0:
        tp_count = 1
    if tp_count > CORE_TP_COUNT:
        tp_count = CORE_TP_COUNT

    tick, step, _ = get_ticks(symbol)

    target_tps = tps[:tp_count]

    # Very small positions: avoid zero-qty orders
    each_default = qdown(size / Decimal(tp_count), step)
    if each_default <= 0:
        # Position too tiny for full split; place a single TP at mid ladder to avoid nonsense.
        if target_tps:
            mid_idx = min(len(target_tps) - 1, tp_count // 2)
            mid_tp = target_tps[mid_idx]
        else:
            # Fallback: just use entry-nearest TP (if ever missing).
            mid_tp = tps[0] if tps else None
        if mid_tp is not None:
            place_reduce_tp(symbol, side_now, qdown(size, step), mid_tp)
        return

    orders_all = _open_orders(symbol)
    tpo = _tp_orders(orders_all, side_now)

    # If no existing TP orders: just place the ladder and reset manual mode.
    if not tpo:
        # If user nuked all TPs, we assume they want the bot back in charge.
        _MANUAL_TP_MODE.pop(symbol, None)
        for px in target_tps:
            place_reduce_tp(symbol, side_now, each_default, px)
        return

    manual_mode = _MANUAL_TP_MODE.get(symbol, False)

    # Possibly enter manual TP mode if we detect significant deviation from our grid.
    if _RESPECT_MANUAL_TPS and not manual_mode:
        if _detect_manual_override(symbol, tpo, target_tps, CORE_TP_COUNT):
            manual_mode = True
            _MANUAL_TP_MODE[symbol] = True
            try:
                send_tg(
                    f"✋ Manual TP override detected for {symbol}. "
                    f"Bot will respect your TP prices until you cancel them or flatten."
                )
            except Exception:
                pass

    # In manual mode: do NOT touch prices; optionally rebalance quantities only.
    if manual_mode and _RESPECT_MANUAL_TPS:
        n = len(tpo)
        if n <= 0:
            # Shouldn't happen; but if it does, treat as no TPs and rebuild next tick.
            _MANUAL_TP_MODE.pop(symbol, None)
            return

        each_manual = qdown(size / Decimal(n), step)
        if each_manual <= 0:
            return

        for o in tpo:
            current_qty = Decimal(o["qty"])
            if current_qty != each_manual:
                _amend_tp_order(
                    symbol,
                    o,
                    new_qty=each_manual,
                    new_price=None,  # DO NOT touch price in manual mode
                )
        return

    # --- Full auto mode (no manual override) below ---

    # Sort both existing orders and target tps by price for consistent pairing
    tps_sorted = sorted(target_tps)
    tpo_sorted = sorted(tpo, key=lambda o: Decimal(o["price"]))

    # We'll manage up to tp_count orders; any extras are ignored (but not canceled).
    managed_orders = tpo_sorted[:tp_count]

    # 1) Amend or place for each target level
    for idx in range(tp_count):
        target_px = tps_sorted[idx]
        if idx < len(managed_orders):
            o = managed_orders[idx]
            current_px = Decimal(o["price"])
            current_qty = Decimal(o["qty"])

            need_price = abs(current_px - target_px) > tick
            need_qty = current_qty != each_default

            if need_price or need_qty:
                _amend_tp_order(
                    symbol,
                    o,
                    new_qty=each_default if need_qty else None,
                    new_price=target_px if need_price else None,
                )
        else:
            # Missing level: place a new TP
            place_reduce_tp(symbol, side_now, each_default, target_px)

    # 2) We intentionally do NOT cancel “extra” orders here to avoid ever
    #    having the book empty. Manual extra TPs are your problem.


def _extract_existing_sl(p: dict) -> Optional[Decimal]:
    """
    Best-effort extraction of the current stop-loss price from a position dict.

    Tries common Bybit field names.
    """
    raw = (
        p.get("stopLoss")
        or p.get("stopLossPrice")
        or p.get("slPrice")
        or p.get("stop_loss")
    )
    if raw in (None, "", "0", 0):
        return None
    try:
        return Decimal(str(raw))
    except Exception:
        return None


def _ensure_exits_for_position(p: dict,
                               seen_state: Dict[str, Tuple[Decimal, Decimal]]) -> None:
    """
    For a single position record, ensure SL + TP ladder exist and are balanced with size.

    Important changes:
      - We NO LONGER skip work just because (avgPrice, size) didn't change.
        We always check that TPs exist; if they're gone, we rebuild immediately.
      - We only send the "Exits set" Telegram message when the position state changes,
        to avoid spamming on every poll.
      - Exit behavior is now strategy-aware:
          • Uses per-sub strategy exit_profile when possible.
          • Supports varying TP counts (up to CORE_TP_COUNT) and trailing SL toggle.
      - Manual SL override:
          • If the existing stopLoss is far from our base SL, we stop touching SL.
    """
    symbol = p["symbol"]
    side_now = p["side"]  # "Buy"/"Sell"
    entry = Decimal(str(p["avgPrice"]))
    size = Decimal(str(p["size"]))

    if size <= 0:
        # Flat; nothing to do
        seen_state.pop(symbol, None)
        _MANUAL_TP_MODE.pop(symbol, None)
        _MANUAL_SL_MODE.pop(symbol, None)
        _TRAIL_STATE.pop(symbol, None)
        return

    prev_state = seen_state.get(symbol)
    state = (entry, size)

    # 0) Exit profile (strategy-aware)
    exit_profile = _get_exit_profile_for_position(p)
    tp_count = int(exit_profile.get("tp_count", CORE_TP_COUNT) or CORE_TP_COUNT)
    trailing_sl = bool(exit_profile.get("trailing_sl", True))

    # 1) Compute grid (base SL + full CORE_TP_COUNT ladder)
    base_sl, tps_full = _compute_exit_grid(symbol, side_now, entry)

    # 1a) Detect / maintain manual SL override
    tick, _step, _ = get_ticks(symbol)
    existing_sl = _extract_existing_sl(p)
    manual_sl_mode = _MANUAL_SL_MODE.get(symbol, False)

    if existing_sl is not None:
        # If we are NOT already in manual SL mode, see if user has moved SL
        # far enough away from our base_sl to treat as override.
        if not manual_sl_mode:
            try:
                if abs(existing_sl - base_sl) > (tick * 2):
                    manual_sl_mode = True
                    _MANUAL_SL_MODE[symbol] = True
                    try:
                        send_tg(
                            f"✋ Manual SL override detected for {symbol}. "
                            f"Bot will respect your SL until you flatten."
                        )
                    except Exception:
                        pass
            except Exception:
                # If anything weird happens, don't explode; just don't mark manual.
                pass
    else:
        # No existing SL; if we were in manual mode, clear it (user removed SL).
        if manual_sl_mode:
            _MANUAL_SL_MODE.pop(symbol, None)
            manual_sl_mode = False

    # 2) Compute trailing SL (if enabled AND not in manual SL mode)
    if manual_sl_mode and existing_sl is not None:
        sl_effective = existing_sl
    else:
        sl_effective = _compute_trailing_sl(
            symbol=symbol,
            side_now=side_now,
            entry=entry,
            base_sl=base_sl,
            tps=tps_full,
            trailing_enabled=trailing_sl,
        )
        # 3) Attach/update SL (trading stop; Bybit handles idempotency)
        set_stop_loss(symbol, sl_effective)

    # 4) Ensure TP ladder via amend-only logic (with manual TP override respect)
    _sync_tp_ladder(symbol, side_now, size, tps_full, tp_count=tp_count)

    # 5) Notify only on state change (entry/size)
    if prev_state != state:
        used_tps = tps_full[:tp_count]
        try:
            send_tg(
                f"🎯 Exits set {symbol} {side_now} | size {size} | "
                f"profile {exit_profile.get('name')} | "
                f"SL {sl_effective} | TPs {', '.join(map(str, used_tps))}"
            )
        except Exception:
            # Don't die on Telegram errors
            pass

    # 6) Track current state
    seen_state[symbol] = state


# ---------------------------------------------------------------------------
# HTTP polling mode (fallback / simple)
# ---------------------------------------------------------------------------

def _loop_http_poll() -> None:
    """
    Legacy but now much safer/faster:
    - Polls positions every POLL_SECONDS
    - On every pass, for each open position, ensures:
        • SL (now trailing-aware, and respects manual override) is attached
        • TP ladder exists (recreated immediately if user nuked it)
    - Uses amend-only TP logic and respects manual TP prices if enabled.
    """
    send_tg(f"🎛 Flashback TP/SL Manager started (HTTP mode, {POLL_SECONDS}s).")
    seen: Dict[str, Tuple[Decimal, Decimal]] = {}

    while True:
        try:
            positions = list_open_positions()
            current_symbols = set()

            for p in positions:
                symbol = p["symbol"]
                current_symbols.add(symbol)
                _ensure_exits_for_position(p, seen_state=seen)

            # prune symbols no longer open
            for s in list(seen.keys()):
                if s not in current_symbols:
                    seen.pop(s, None)
                    _MANUAL_TP_MODE.pop(s, None)
                    _MANUAL_SL_MODE.pop(s, None)
                    _TRAIL_STATE.pop(s, None)

            time.sleep(POLL_SECONDS)
        except Exception as e:
            send_tg(f"[TP/SL HTTP] {e}")
            time.sleep(5)


# ---------------------------------------------------------------------------
# WebSocket mode: private stream for positions
# ---------------------------------------------------------------------------

def _ws_auth_payload() -> dict:
    """
    Build auth message for private WS:
      op: "auth"
      args: [api_key, expires, signature]
    Signature = HMAC_SHA256(secret, f"{api_key}{expires}") in hex.
    """
    if not WS_KEY or not WS_SECRET:
        raise RuntimeError("Missing BYBIT_MAIN_TRADE_KEY / BYBIT_MAIN_TRADE_SECRET for WS auth")

    expires = int(time.time() * 1000) + 5000  # ms in future
    msg = f"{WS_KEY}{expires}"
    sig = hmac.new(
        WS_SECRET.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "op": "auth",
        "args": [WS_KEY, str(expires), sig],
    }


def _handle_ws_position_message(msg: dict,
                                seen: Dict[str, Tuple[Decimal, Decimal]]) -> None:
    """
    Handle a Bybit private 'position' topic push.
    Expected shape (simplified):
      {
        "topic": "position",
        "data": [
          {
            "symbol": "BTCUSDT",
            "category": "linear",
            "side": "Buy",
            "size": "0.001",
            "avgPrice": "61234.5",
            "stopLoss": "61200.0",
            ...
          },
          ...
        ]
      }
    """
    topic = msg.get("topic", "")
    if "position" not in topic:
        return

    data = msg.get("data", [])
    if isinstance(data, dict):
        data = [data]

    current_symbols = set()

    for p in data:
        if p.get("category") != CATEGORY:
            continue
        symbol = p.get("symbol")
        if not symbol:
            continue
        current_symbols.add(symbol)

        size = Decimal(str(p.get("size", "0")))
        if size <= 0:
            # Flat now; clear state
            seen.pop(symbol, None)
            _MANUAL_TP_MODE.pop(symbol, None)
            _MANUAL_SL_MODE.pop(symbol, None)
            _TRAIL_STATE.pop(symbol, None)
            continue

        # Normalize to match REST shape (and keep any sub_uid-ish keys intact)
        norm = {
            "symbol": symbol,
            "side": p.get("side"),
            "avgPrice": p.get("avgPrice"),
            "size": p.get("size"),
            # Pass through existing SL field if present
            "stopLoss": p.get("stopLoss") or p.get("stopLossPrice") or p.get("slPrice"),
            # Pass through potential subaccount identifiers for strategy lookup
            "sub_uid": p.get("sub_uid") or p.get("subAccountId") or p.get("accountId") or p.get("subId"),
        }
        _ensure_exits_for_position(norm, seen_state=seen)

    # Clean up any symbols that disappeared entirely
    for s in list(seen.keys()):
        if s not in current_symbols:
            seen.pop(s, None)
            _MANUAL_TP_MODE.pop(s, None)
            _MANUAL_SL_MODE.pop(s, None)
            _TRAIL_STATE.pop(s, None)


def _loop_ws() -> None:
    """
    WebSocket-only main loop:
    - Connects to wss://stream.bybit.com/v5/private
    - Authenticates
    - Subscribes to "position" private topic
    - On each push, enforces SL + TP ladder with amend-only logic.

    Note: TP recreation after cancel still works in WS mode, but if the
    exchange stops sending position events while flat, HTTP mode will
    remain the more predictable fallback.
    """
    if websocket is None:
        raise RuntimeError("websocket-client is not installed. pip install websocket-client")

    send_tg("🎛 Flashback TP/SL Manager started (WebSocket mode).")

    seen: Dict[str, Tuple[Decimal, Decimal]] = {}

    while True:
        ws = None
        try:
            # Short-ish timeout so we don't block forever on recv
            ws = websocket.create_connection(WS_PRIVATE_URL, timeout=5)
            # Auth
            auth_msg = _ws_auth_payload()
            ws.send(json.dumps(auth_msg))

            # Wait for auth OK
            raw = ws.recv()
            resp = json.loads(raw)
            if resp.get("retCode") != 0:
                raise RuntimeError(f"WS auth failed: {resp}")

            # Subscribe to private position topic
            sub = {"op": "subscribe", "args": ["position"]}
            ws.send(json.dumps(sub))

            last_ping = time.time()

            while True:
                # Keepalive ping every ~15s
                now = time.time()
                if now - last_ping > 15:
                    ws.send(json.dumps({"op": "ping"}))
                    last_ping = now

                raw = ws.recv()
                if not raw:
                    # Connection closed
                    raise RuntimeError("WS closed")

                msg = json.loads(raw)

                # Ignore pongs/heartbeat noise
                if msg.get("op") in ("pong", "ping"):
                    continue

                # Position pushes
                if "topic" in msg and "position" in msg["topic"]:
                    _handle_ws_position_message(msg, seen=seen)

        except Exception as e:
            send_tg(f"[TP/SL WS] reconnecting after error: {e}")
            time.sleep(3)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def loop():
    """
    Entry point called by supervisor.
    Chooses WebSocket mode or HTTP poll mode depending on TPM_USE_WEBSOCKET.

    Behavior summary:
      - HTTP mode:
          • Polls positions every POLL_SECONDS.
          • Ensures trailing SL + strategy-aware TP ladder on every pass.
          • Respect manual SL (if moved off our base by >2 ticks) and manual TPs.
      - WS mode:
          • Reacts to private 'position' pushes near-instantly.
          • Falls back to HTTP mode on hard WS failure.

    For your "absolutely unacceptable" TP-gap problem:
      - HTTP mode still ALWAYS checks for missing TPs every POLL_SECONDS.
      - If all TPs are canceled on an open position, they are rebuilt on the
        very next poll, regardless of size/avgPrice changes.
    """
    if USE_WS:
        try:
            _loop_ws()
        except Exception as e:
                # Hard failure in WS mode -> fall back to HTTP polling
            send_tg(f"[TP/SL] WS hard failure, falling back to HTTP mode: {e}")
            _loop_http_poll()
    else:
        _loop_http_poll()


if __name__ == "__main__":
    loop()
