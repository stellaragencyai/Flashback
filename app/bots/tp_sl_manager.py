# app/bots/tp_sl_manager.py
# Flashback — TP/SL Manager v4
#
# Key upgrades:
# - Uses Bybit V5 PRIVATE WEBSOCKET for near-instant position updates (optional).
# - NEVER nukes the whole TP ladder on size change:
#     • Existing TP orders are amended in place (qty/price).
#     • Only missing TPs are added; we do NOT call cancel_all() in normal flow.
# - Keeps SL attached via trading-stop.
# - ATR caching to avoid hammering indicators.
#
# New behavior (v4):
#   1) "No TP" watchdog:
#      - On every cycle, for every open position, we inspect open orders.
#      - If ALL TP orders have been canceled, we IMMEDIATELY rebuild the TP ladder
#        (5 exits or a single mid TP for tiny positions), regardless of whether
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
#   ENV knobs:
#     TPM_USE_WEBSOCKET=true|false
#     TPM_POLL_SECONDS=2        (HTTP mode only; also used as cadence for TP checks)
#     TPM_ATR_CACHE_SEC=60
#     TPM_RESPECT_MANUAL_TPS=true|false   (default true)
#
#   ATR_MULT, TP5_MAX_ATR_MULT, TP5_MAX_PCT, R_MIN_TICKS come from flashback_common or defaults.

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

# ATR cache: symbol -> (ts, atr)
_ATR_CACHE_TTL = int(os.getenv("TPM_ATR_CACHE_SEC", "60"))
_ATR_CACHE: Dict[str, Tuple[float, Decimal]] = {}

# Manual TP override per symbol: if True, we do NOT amend TP prices for that symbol.
_MANUAL_TP_MODE: Dict[str, bool] = {}


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
    Returns (stop_loss_price, [tp1..tp5]) snapped to valid tick.

    Spacing logic:
      - Base R = max(ATR * ATR_MULT, R_MIN_TICKS * tick)
      - tp_i = entry ± i*R depending on side
      - tp5 capped by:
          • TP5_MAX_ATR_MULT * ATR distance
          • TP5_MAX_PCT% of entry
    """
    tick, _step, _min_notional = get_ticks(symbol)

    atr = _get_atr(symbol, entry)
    if atr <= 0:
        # Paranoid fallback – use small percent band directly
        atr = entry * Decimal("0.002")

    # Base R distance
    R = atr * Decimal(ATR_MULT)

    # Enforce minimum ticks
    min_R = tick * Decimal(R_MIN_TICKS)
    if R < min_R:
        R = min_R

    # Cap tp5
    max_tp5_dist_atr = atr * Decimal(TP5_MAX_ATR_MULT)
    max_tp5_dist_pct = entry * (Decimal(TP5_MAX_PCT) / Decimal(100))
    max_tp5_dist = min(max_tp5_dist_atr, max_tp5_dist_pct)

    if side_now.lower() == "buy":
        sl = entry - R
        tps = [entry + i * R for i in range(1, 6)]
        if (tps[-1] - entry) > max_tp5_dist:
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
                            target_tps: List[Decimal]) -> bool:
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

    n = min(len(cur_prices), len(tgt_sorted), 5)
    if n == 0:
        return False

    mismatches = 0
    for i in range(n):
        if abs(cur_prices[i] - tgt_sorted[i]) > (tick * 2):
            mismatches += 1

    # Two or more TPs significantly off-grid => treating as manual override.
    return mismatches >= 2


def _sync_tp_ladder(symbol: str, side_now: str, size: Decimal, tps: List[Decimal]) -> None:
    """
    Ensure we have a 5-TP ladder, **without** ever nuking the book.

    Behavior:
      - If no TPs: place fresh 5 reduce-only orders (or single mid TP if tiny).
      - If some TPs:
          • If TPM_RESPECT_MANUAL_TPS=true and we detect a manual override,
            we DO NOT amend TP prices; we only (optionally) rebalance qty.
          • Otherwise: we amend TPs in place to match target prices / equal qty splits.
      - Extra TPs (from manual tinkering) are left alone; we only manage up to 5 core levels.
    """
    tick, step, _ = get_ticks(symbol)

    # Very small positions: avoid zero-qty orders
    each_default = qdown(size / Decimal(5), step)
    if each_default <= 0:
        # Position too tiny for 5-way split; place a single TP at tp3 to avoid nonsense.
        mid_tp = tps[2]  # center of the ladder
        place_reduce_tp(symbol, side_now, qdown(size, step), mid_tp)
        return

    orders_all = _open_orders(symbol)
    tpo = _tp_orders(orders_all, side_now)

    # If no existing TP orders: just place the ladder and reset manual mode.
    if not tpo:
        # If user nuked all TPs, we assume they want the bot back in charge.
        _MANUAL_TP_MODE.pop(symbol, None)
        for px in tps:
            place_reduce_tp(symbol, side_now, each_default, px)
        return

    manual_mode = _MANUAL_TP_MODE.get(symbol, False)

    # Possibly enter manual TP mode if we detect significant deviation from our grid.
    if _RESPECT_MANUAL_TPS and not manual_mode:
        if _detect_manual_override(symbol, tpo, tps):
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
    tps_sorted = sorted(tps)
    tpo_sorted = sorted(tpo, key=lambda o: Decimal(o["price"]))

    # We'll manage up to 5 orders; any extras are ignored (but not canceled).
    managed_orders = tpo_sorted[:5]

    # 1) Amend or place for each target level
    for idx in range(5):
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


def _ensure_exits_for_position(p: dict,
                               seen_state: Dict[str, Tuple[Decimal, Decimal]]) -> None:
    """
    For a single position record, ensure SL + 5TP exist and are balanced with size.

    Important changes:
      - We NO LONGER skip work just because (avgPrice, size) didn't change.
        We always check that TPs exist; if they're gone, we rebuild immediately.
      - We only send the "Exits set" Telegram message when the position state changes,
        to avoid spamming on every poll.
    """
    symbol = p["symbol"]
    side_now = p["side"]  # "Buy"/"Sell"
    entry = Decimal(str(p["avgPrice"]))
    size = Decimal(str(p["size"]))

    if size <= 0:
        # Flat; nothing to do
        seen_state.pop(symbol, None)
        _MANUAL_TP_MODE.pop(symbol, None)
        return

    prev_state = seen_state.get(symbol)
    state = (entry, size)

    # 1) Compute grid
    sl, tps = _compute_exit_grid(symbol, side_now, entry)

    # 2) Attach/update SL (trading stop; Bybit handles idempotency)
    set_stop_loss(symbol, sl)

    # 3) Ensure TP ladder via amend-only logic (with manual override respect)
    _sync_tp_ladder(symbol, side_now, size, tps)

    # 4) Notify only on state change (entry/size)
    if prev_state != state:
        send_tg(f"🎯 Exits set {symbol} {side_now} | size {size} | SL {sl} | TPs {', '.join(map(str, tps))}")

    # 5) Track current state
    seen_state[symbol] = state


# ---------------------------------------------------------------------------
# HTTP polling mode (fallback / simple)
# ---------------------------------------------------------------------------

def _loop_http_poll() -> None:
    """
    Legacy but now much safer/faster:
    - Polls positions every POLL_SECONDS
    - On every pass, for each open position, ensures:
        • SL is attached
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
            continue

        # Normalize to match REST shape
        norm = {
            "symbol": symbol,
            "side": p.get("side"),
            "avgPrice": p.get("avgPrice"),
            "size": p.get("size"),
        }
        _ensure_exits_for_position(norm, seen_state=seen)

    # Clean up any symbols that disappeared entirely
    for s in list(seen.keys()):
        if s not in current_symbols:
            seen.pop(s, None)
            _MANUAL_TP_MODE.pop(s, None)


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

    For your "absolutely unacceptable" TP-gap problem:
      - HTTP mode now ALWAYS checks for missing TPs every POLL_SECONDS.
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
