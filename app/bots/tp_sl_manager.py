# app/bots/tp_sl_manager.py
# Flashback — TP/SL Manager v6.6 (position-bus aware, strategy lookup fixed)
#
# Mode summary
# ------------
# HTTP mode:
#   - Uses app.core.position_bus.get_positions_snapshot(...)
#   - That prefers WS-fed state/positions_bus.json if fresh
#   - Falls back to REST list_open_positions(category="linear") for label "main"
#
# WS mode (unchanged):
#   - Connects directly to BYBIT_WS_PRIVATE_URL
#   - Subscribes to "position" private topic
#   - Feeds positions from WS pushes only
#
# Keeps:
#   - Exit profiles (standard_7 / standard_5 / aggressive_7 / scalp_3)
#   - 7-TP ladders, manual TP/SL override logic
#   - Trailing SL, ATR-based spacing, safety gap from market
#   - HTTP polling + optional direct WS mode

import os
import time
import json
from decimal import Decimal
from typing import Dict, Tuple, List, Optional

from app.core.flashback_common import (
    bybit_get,
    bybit_post,
    send_tg,
    get_ticks,
    psnap,
    qdown,
    last_price,
    atr14,
    set_stop_loss,
    cancel_all,      # kept for emergencies only; not used in normal flow
    place_reduce_tp,
    BYBIT_WS_PRIVATE_URL,
    build_ws_auth_payload_main,
    record_heartbeat,
    alert_bot_error,
)

# Position bus (HTTP mode reads positions via this)
from app.core.position_bus import get_positions_snapshot as bus_get_positions_snapshot

# Optional websocket support (websocket-client) for direct TP/SL WS mode
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

# WebSocket toggle (direct private WS for TP/SL manager itself)
USE_WS = os.getenv("TPM_USE_WEBSOCKET", "false").strip().lower() == "true"

# Respect manual TP modifications (prices) or not
_RESPECT_MANUAL_TPS = os.getenv("TPM_RESPECT_MANUAL_TPS", "true").strip().lower() == "true"

# Trailing SL config
_TRAIL_R_MULT = Decimal(os.getenv("TPM_TRAIL_R_MULT", "1.0"))

# SL distance multiplier (relative to base R)
SL_R_MULT = Decimal(os.getenv("TPM_SL_R_MULT", "2.2"))

# Minimum TP gap in ticks from current price for auto-managed TPs
try:
    _MIN_TP_GAP_TICKS = int(os.getenv("TPM_MIN_TP_GAP_TICKS", "5"))
except Exception:
    _MIN_TP_GAP_TICKS = 5

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
          • TP5_MAX_ATR_MULT * ATR distance
          • TP5_MAX_PCT% of entry
      - If cap is hit, R_tp is compressed so that tpn - entry == max_tp_dist.
    """
    tick, _step, _min_notional = get_ticks(symbol)

    atr = _get_atr(symbol, entry)
    if atr <= 0:
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

    Supports two shapes in strategies.yaml:

      exit_profile:
        name: standard_7
        tp_count: 7
        trailing_sl: true

      exit_profile: "standard_5"
    """
    profile = dict(DEFAULT_EXIT_PROFILE)

    if strat_mod is None:
        return profile

    # sub_uid field may be present depending on how list_open_positions / WS is wired.
    sub_uid = (
        p.get("sub_uid")
        or p.get("subAccountId")
        or p.get("accountId")
        or p.get("subId")
    )
    if not sub_uid:
        return profile

    # Cleaned-up strategy lookup (no NameError hack)
    try:
        strat = strat_mod.get_strategy_for_sub(str(sub_uid))
    except Exception:
        strat = None

    if not strat:
        return profile

    cfg = strat.get("exit_profile") or strat.get("exitProfile")

    # Helper for mapping profile name -> tp_count / trailing flag
    def _apply_name_only(name_raw: str) -> None:
        name_norm = name_raw.strip().lower()
        if name_norm == "standard_7":
            profile["name"] = "standard_7"
            profile["tp_count"] = CORE_TP_COUNT
            profile["trailing_sl"] = True
        elif name_norm == "standard_5":
            profile["name"] = "standard_5"
            profile["tp_count"] = 5
            profile["trailing_sl"] = True
        elif name_norm == "aggressive_7":
            profile["name"] = "aggressive_7"
            profile["tp_count"] = CORE_TP_COUNT
            profile["trailing_sl"] = True
        elif name_norm == "scalp_3":
            profile["name"] = "scalp_3"
            profile["tp_count"] = 3
            profile["trailing_sl"] = True
        else:
            # Unknown name: keep default but store label
            profile["name"] = name_raw

    if isinstance(cfg, dict):
        name = cfg.get("name")
        if isinstance(name, str):
            _apply_name_only(name)

        if "tp_count" in cfg:
            try:
                tp_count_val = int(cfg["tp_count"])
                if tp_count_val > 0:
                    # Hard cap at CORE_TP_COUNT so grid doesn't exceed 7 rungs
                    profile["tp_count"] = min(tp_count_val, CORE_TP_COUNT)
            except Exception:
                pass

        if "trailing_sl" in cfg:
            profile["trailing_sl"] = bool(cfg["trailing_sl"])

    elif isinstance(cfg, str):
        _apply_name_only(cfg)

    return profile


def _safe_tp_price(symbol: str, side_now: str, target_px: Decimal) -> Decimal:
    """
    Enforce a minimum distance between TP price and current market price.
    """
    try:
        if _MIN_TP_GAP_TICKS <= 0:
            return target_px

        mkt = Decimal(str(last_price(symbol)))
        if mkt <= 0:
            return target_px

        tick, _step, _ = get_ticks(symbol)
        gap = tick * Decimal(_MIN_TP_GAP_TICKS)

        if side_now.lower() == "buy":
            min_px = mkt + gap
            if target_px <= min_px:
                target_px = min_px
        else:
            max_px = mkt - gap
            if target_px >= max_px:
                target_px = max_px

        return psnap(target_px, tick)
    except Exception:
        return target_px


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
    """
    if not trailing_enabled or _TRAIL_R_MULT <= 0:
        return base_sl

    try:
        price = Decimal(str(last_price(symbol)))
    except Exception:
        return base_sl

    if price <= 0:
        return base_sl

    if tps:
        R = abs(tps[0] - entry)
    else:
        R = abs(entry - base_sl)

    if R <= 0:
        return base_sl

    trail_dist = R * _TRAIL_R_MULT

    state = _TRAIL_STATE.get(symbol)
    if state is None or state.get("entry") != entry or state.get("base_sl") != base_sl:
        state = {
            "entry": entry,
            "base_sl": base_sl,
            "best": price,
        }
    else:
        best = state.get("best", entry)
        if side_now.lower() == "buy":
            if price > best:
                best = price
        else:
            if price < best:
                best = price
        state["best"] = best

    best = state["best"]

    if side_now.lower() == "buy":
        sl_candidate = best - trail_dist
        sl_new = max(base_sl, sl_candidate)
    else:
        sl_candidate = best + trail_dist
        sl_new = min(base_sl, sl_candidate)

    tick, _step, _ = get_ticks(symbol)
    sl_new = psnap(sl_new, tick)

    _TRAIL_STATE[symbol] = state
    return sl_new


def _amend_tp_order(
    symbol: str,
    order: dict,
    new_qty: Optional[Decimal],
    new_price: Optional[Decimal],
    side_now: Optional[str] = None,
) -> None:
    """
    Amend a single TP order in place via REST /v5/order/amend.
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
        return

    if new_price is not None:
        if side_now is not None:
            new_price = _safe_tp_price(symbol, side_now, new_price)
        body["price"] = str(new_price)
    if new_qty is not None:
        body["qty"] = str(new_qty)

    try:
        bybit_post("/v5/order/amend", body)
    except Exception as e:
        alert_bot_error("tp_sl_manager", f"{symbol} amend error: {e}", "ERROR")


def _detect_manual_override(
    symbol: str,
    tpo: List[dict],
    target_tps: List[Decimal],
    core_count: int,
) -> bool:
    """
    Heuristic: if a majority of TP prices deviate from our ideal grid by more than
    ~2 ticks, assume the user manually moved them and enter manual TP mode.
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

    return mismatches >= 2


def _sync_tp_ladder(
    symbol: str,
    side_now: str,
    size: Decimal,
    tps: List[Decimal],
    tp_count: int,
) -> None:
    """
    Ensure we have a TP ladder (up to CORE_TP_COUNT), **without** ever nuking the book.

    Notes:
      - Swallows Bybit "not enough balance / min size" errors when placing TPs.
        Those are normal when size is tiny; we warn once via Telegram and move on.
    """
    if tp_count <= 0:
        tp_count = 1
    if tp_count > CORE_TP_COUNT:
        tp_count = CORE_TP_COUNT

    tick, step, _ = get_ticks(symbol)
    target_tps = tps[:tp_count]

    each_default = qdown(size / Decimal(tp_count), step)
    if each_default <= 0:
        # Too small to split properly; fall back to a single mid TP if possible.
        if target_tps:
            mid_idx = min(len(target_tps) - 1, tp_count // 2)
            mid_tp = target_tps[mid_idx]
        else:
            mid_tp = tps[0] if tps else None
        if mid_tp is not None:
            safe_mid = _safe_tp_price(symbol, side_now, mid_tp)
            try:
                place_reduce_tp(symbol, side_now, qdown(size, step), safe_mid)
            except Exception as e:
                # Do NOT crash the loop; just warn once.
                alert_bot_error("tp_sl_manager", f"{symbol} single-TP create error: {e}", "WARN")
        return

    orders_all = _open_orders(symbol)
    tpo = _tp_orders(orders_all, side_now)

    # No TPs at all: build a fresh ladder
    if not tpo:
        _MANUAL_TP_MODE.pop(symbol, None)
        for px in target_tps:
            safe_px = _safe_tp_price(symbol, side_now, px)
            try:
                place_reduce_tp(symbol, side_now, each_default, safe_px)
            except Exception as e:
                # e.g. Bybit retCode 110007: not enough margin / min qty
                alert_bot_error("tp_sl_manager", f"{symbol} TP create error: {e}", "WARN")
        return

    manual_mode = _MANUAL_TP_MODE.get(symbol, False)

    # Manual TP override detection
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

    # Manual mode: keep prices, only rebalance qty if needed
    if manual_mode and _RESPECT_MANUAL_TPS:
        n = len(tpo)
        if n <= 0:
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
                    new_price=None,
                    side_now=None,
                )
        return

    # --- Full auto mode (no manual override) below ---

    tps_sorted = sorted(target_tps)
    tpo_sorted = sorted(tpo, key=lambda o: Decimal(o["price"]))

    managed_orders = tpo_sorted[:tp_count]

    for idx in range(tp_count):
        raw_target_px = tps_sorted[idx]
        target_px = _safe_tp_price(symbol, side_now, raw_target_px)

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
                    side_now=side_now,
                )
        else:
            # Extra TP rung we want but don't have yet
            try:
                place_reduce_tp(symbol, side_now, each_default, target_px)
            except Exception as e:
                alert_bot_error("tp_sl_manager", f"{symbol} extra TP create error: {e}", "WARN")


def _extract_existing_sl(p: dict) -> Optional[Decimal]:
    """
    Best-effort extraction of the current stop-loss price from a position dict.
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


def _ensure_exits_for_position(
    p: dict,
    seen_state: Dict[str, Tuple[Decimal, Decimal]],
) -> None:
    """
    For a single position record, ensure SL + TP ladder exist and are balanced with size.
    """
    symbol = p["symbol"]
    side_now = p["side"]  # "Buy"/"Sell"
    entry = Decimal(str(p["avgPrice"]))

    size = Decimal(str(p["size"]))

    if size <= 0:
        seen_state.pop(symbol, None)
        _MANUAL_TP_MODE.pop(symbol, None)
        _MANUAL_SL_MODE.pop(symbol, None)
        _TRAIL_STATE.pop(symbol, None)
        return

    prev_state = seen_state.get(symbol)
    state = (entry, size)

    exit_profile = _get_exit_profile_for_position(p)
    tp_count = int(exit_profile.get("tp_count", CORE_TP_COUNT) or CORE_TP_COUNT)
    trailing_sl = bool(exit_profile.get("trailing_sl", True))

    base_sl, tps_full = _compute_exit_grid(symbol, side_now, entry)

    tick, _step, _ = get_ticks(symbol)
    existing_sl = _extract_existing_sl(p)
    manual_sl_mode = _MANUAL_SL_MODE.get(symbol, False)

    if existing_sl is not None:
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
                pass
    else:
        if manual_sl_mode:
            _MANUAL_SL_MODE.pop(symbol, None)
            manual_sl_mode = False

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
        set_stop_loss(symbol, sl_effective)

    _sync_tp_ladder(symbol, side_now, size, tps_full, tp_count=tp_count)

    if prev_state != state:
        used_tps = tps_full[:tp_count]
        try:
            send_tg(
                f"🎯 Exits set {symbol} {side_now} | size {size} | "
                f"profile {exit_profile.get('name')} | "
                f"SL {sl_effective} | TPs {', '.join(map(str, used_tps))}"
            )
        except Exception:
            pass

    seen_state[symbol] = state


# ---------------------------------------------------------------------------
# HTTP polling mode (position-bus powered)
# ---------------------------------------------------------------------------

def _loop_http_poll() -> None:
    """
    Polls positions every POLL_SECONDS and ensures exits are attached.

    HTTP mode now uses position_bus.get_positions_snapshot(), which:
      - Reads state/positions_bus.json if fresh enough.
      - Falls back to REST list_open_positions(category="linear") for MAIN
        when snapshot is missing/stale, and updates positions_bus.json.
    """
    send_tg(
        f"🎛 Flashback TP/SL Manager started (HTTP + position_bus mode, {POLL_SECONDS}s)."
    )
    seen: Dict[str, Tuple[Decimal, Decimal]] = {}

    while True:
        record_heartbeat("tp_sl_manager")
        try:
            # label=None means "use ACCOUNT_LABEL" inside position_bus
            positions = bus_get_positions_snapshot(
                label=None,
                category=CATEGORY,
                max_age_seconds=None,      # let position_bus decide age or use its default
                allow_rest_fallback=True,
            )
            current_symbols = set()

            for p in positions:
                symbol = p.get("symbol")
                if not symbol:
                    continue
                current_symbols.add(symbol)
                _ensure_exits_for_position(p, seen_state=seen)

            for s in list(seen.keys()):
                if s not in current_symbols:
                    seen.pop(s, None)
                    _MANUAL_TP_MODE.pop(s, None)
                    _MANUAL_SL_MODE.pop(s, None)
                    _TRAIL_STATE.pop(s, None)

            time.sleep(POLL_SECONDS)
        except Exception as e:
            alert_bot_error("tp_sl_manager", f"HTTP loop error: {e}", "ERROR")
            time.sleep(5)


# ---------------------------------------------------------------------------
# WebSocket mode: private stream for positions (direct WS)
# ---------------------------------------------------------------------------

def _handle_ws_position_message(
    msg: dict,
    seen: Dict[str, Tuple[Decimal, Decimal]],
) -> None:
    """
    Handle a Bybit private 'position' topic push.
    """
    topic = msg.get("topic", "")
    if "position" not in topic:
        return

    data = msg.get("data", [])
    if isinstance(data, dict):
        data = [data]

    current_symbols = set()

    for p in data:
        if str(p.get("category", "")).lower() != CATEGORY:
            continue
        symbol = p.get("symbol")
        if not symbol:
            continue
        current_symbols.add(symbol)

        size = Decimal(str(p.get("size", "0")))
        if size <= 0:
            seen.pop(symbol, None)
            _MANUAL_TP_MODE.pop(symbol, None)
            _MANUAL_SL_MODE.pop(symbol, None)
            _TRAIL_STATE.pop(symbol, None)
            continue

        norm = {
            "symbol": symbol,
            "side": p.get("side"),
            "avgPrice": p.get("avgPrice"),
            "size": p.get("size"),
            "stopLoss": p.get("stopLoss") or p.get("stopLossPrice") or p.get("slPrice"),
            "sub_uid": p.get("sub_uid") or p.get("subAccountId") or p.get("accountId") or p.get("subId"),
        }
        _ensure_exits_for_position(norm, seen_state=seen)

    for s in list(seen.keys()):
        if s not in current_symbols:
            seen.pop(s, None)
            _MANUAL_TP_MODE.pop(s, None)
            _MANUAL_SL_MODE.pop(s, None)
            _TRAIL_STATE.pop(s, None)


def _loop_ws() -> None:
    """
    WebSocket-only main loop:
    - Connects to BYBIT_WS_PRIVATE_URL
    - Authenticates using shared auth builder
    - Subscribes to "position" private topic
    """
    if websocket is None:
        raise RuntimeError("websocket-client is not installed. pip install websocket-client")

    send_tg("🎛 Flashback TP/SL Manager started (WebSocket mode).")

    seen: Dict[str, Tuple[Decimal, Decimal]] = {}

    while True:
        ws = None
        try:
            ws = websocket.create_connection(BYBIT_WS_PRIVATE_URL, timeout=5)

            # Auth using shared helper (correct Bybit v5 format)
            auth_msg = build_ws_auth_payload_main()
            ws.send(json.dumps(auth_msg))

            raw = ws.recv()
            resp = json.loads(raw)
            if resp.get("success") is False or resp.get("retCode", 0) != 0:
                raise RuntimeError(f"WS auth failed: {resp}")

            sub = {"op": "subscribe", "args": ["position"]}
            ws.send(json.dumps(sub))

            last_ping = time.time()

            while True:
                record_heartbeat("tp_sl_manager")

                now = time.time()
                if now - last_ping > 15:
                    ws.send(json.dumps({"op": "ping"}))
                    last_ping = now

                raw = ws.recv()
                if not raw:
                    raise RuntimeError("WS closed")

                msg = json.loads(raw)

                if msg.get("op") in ("pong", "ping"):
                    continue

                if "topic" in msg and "position" in msg["topic"]:
                    _handle_ws_position_message(msg, seen=seen)

        except Exception as e:
            alert_bot_error("tp_sl_manager", f"WS loop error: {e}", "ERROR")
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

    - HTTP mode: uses position_bus.get_positions_snapshot() (WS-fed file + REST fallback).
    - WS mode  : uses direct private WS and ignores position_bus.
    """
    if USE_WS:
        try:
            _loop_ws()
        except Exception as e:
            alert_bot_error("tp_sl_manager", f"WS hard failure, falling back to HTTP: {e}", "ERROR")
            _loop_http_poll()
    else:
        _loop_http_poll()


if __name__ == "__main__":
    loop()
