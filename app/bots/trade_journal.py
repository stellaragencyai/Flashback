# app/bots/trade_journal.py
# Flashback — Trade Journal (Main, v3 — richer metrics + trade rating)
#
# What it does
# - Streams executions with a persistent cursor so we don't miss partials.
# - Classifies each new fill as ENTRY/ADD or PARTIAL EXIT based on current position side.
# - On first entry per symbol: captures a rich OPEN snapshot:
#       • symbol, side, direction (LONG/SHORT)
#       • entry_price, size, leverage, init_margin
#       • stop_loss, tp_prices, avg_rr_5
#       • equity_at_open, entry_notional_usd
#       • risk_per_unit, risk_usd, potential_reward_usd
#       • entry_order_type (Market/Limit/..) and entry_liquidity (MAKER/TAKER)
#       • timestamps: ts_open_ms, ts_open_iso
#       • num_adds, num_partials
# - On partial exits: sends PnL-ish updates with remaining size.
# - On full close (size -> 0): fetches closed PnL, composes a final trade summary,
#   appends JSONL with:
#       • ts_close_ms, ts_close_iso
#       • duration_ms, duration_human
#       • realized_pnl, realized_rr, result (WIN/LOSS/BREAKEVEN)
#       • equity_after_close
#       • rating_score (1–10) + rating_reason
#
# Files
# - app/state/journal.jsonl         (append-only ledger of closed trades)
# - app/state/journal_cursor.json   (stores Bybit executions cursor)
# - app/state/journal_open.json     (last known open snapshot per symbol)

import time
import traceback
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, Tuple, List, Optional, Any

import orjson

from app.core.flashback_common import (
    bybit_get,
    list_open_positions,
    get_equity_usdt,
    get_ticks,
    psnap,
    atr14,
)

from app.core.notifier_bot import get_notifier

CATEGORY = "linear"
POLL_SECONDS = 3

JOURNAL_LEDGER = Path("app/state/journal.jsonl")
CURSOR_PATH    = Path("app/state/journal_cursor.json")
OPEN_STATE     = Path("app/state/journal_open.json")

# Journal metadata
ACCOUNT_LABEL     = "MAIN"
JOURNAL_VERSION   = 3  # bumped for rating

# Use dedicated journal notifier channel
tg = get_notifier("journal")

# Import spacing config from common if present
try:
    from app.core.flashback_common import ATR_MULT, TP5_MAX_ATR_MULT, TP5_MAX_PCT, R_MIN_TICKS
except Exception:
    ATR_MULT = Decimal("1.0")
    TP5_MAX_ATR_MULT = Decimal("3.0")
    TP5_MAX_PCT = Decimal("6.0")
    R_MIN_TICKS = 3

# ---------- util ----------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_iso(ts_ms: int) -> str:
    """
    Convert epoch ms to local ISO-ish string.
    Uses localtime; TZ from OS / env.
    """
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ts_ms / 1000))
    except Exception:
        return str(ts_ms)


def _write_jsonl(row: dict) -> None:
    JOURNAL_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with JOURNAL_LEDGER.open("ab") as f:
        f.write(orjson.dumps(row) + b"\n")


def _load_cursor() -> Optional[str]:
    try:
        data = orjson.loads(CURSOR_PATH.read_bytes())
        cur = data.get("cursor", "") or ""
        return cur or None
    except Exception:
        return None


def _save_cursor(cur: Optional[str]) -> None:
    CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    CURSOR_PATH.write_bytes(orjson.dumps({"cursor": cur or ""}))


def _load_open_state() -> Dict[str, dict]:
    try:
        return orjson.loads(OPEN_STATE.read_bytes())
    except Exception:
        return {}


def _save_open_state(state: Dict[str, dict]) -> None:
    OPEN_STATE.parent.mkdir(parents=True, exist_ok=True)
    OPEN_STATE.write_bytes(orjson.dumps(state))


def _human_dur(ms: int) -> str:
    s = ms // 1000
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _direction_from_side(side: str) -> str:
    side = (side or "").strip().lower()
    if side == "buy":
        return "LONG"
    if side == "sell":
        return "SHORT"
    return "UNKNOWN"


def _fmt_dec(x: Optional[Decimal], places: int = 2) -> Optional[str]:
    if x is None:
        return None
    q = Decimal("1").scaleb(-places)  # 10^-places
    return str(x.quantize(q, rounding=ROUND_DOWN))

# ---------- grid inference (fallback if TPs not yet visible) ----------

def _kline_infer_grid(symbol: str, side_now: str, entry: Decimal) -> Tuple[Decimal, List[Decimal]]:
    tick, _step, _ = get_ticks(symbol)
    atr = atr14(symbol, interval="60")
    if atr <= 0:
        atr = entry * Decimal("0.002")
    R = atr * Decimal(ATR_MULT)
    min_R = tick * Decimal(R_MIN_TICKS)
    if R < min_R:
        R = min_R
    max_tp5_dist_atr = atr * Decimal(TP5_MAX_ATR_MULT)
    max_tp5_dist_pct = entry * (Decimal(TP5_MAX_PCT) / Decimal(100))
    max_tp5_dist = min(max_tp5_dist_atr, max_tp5_dist_pct)

    if side_now.lower() == "buy":
        sl = psnap(entry - R, tick)
        tps = [psnap(entry + i * R, tick) for i in range(1, 6)]
        if (tps[-1] - entry) > max_tp5_dist:
            step = max_tp5_dist / Decimal(5)
            tps = [psnap(entry + i * step, tick) for i in range(1, 6)]
    else:
        sl = psnap(entry + R, tick)
        tps = [psnap(entry - i * R, tick) for i in range(1, 6)]
        if (entry - tps[-1]) > max_tp5_dist:
            step = max_tp5_dist / Decimal(5)
            tps = [psnap(entry - i * step, tick) for i in range(1, 6)]
    return sl, tps


def _avg_rr(entry: Decimal, sl: Optional[Decimal], tps: List[Decimal]) -> Optional[Decimal]:
    if sl is None or entry == sl:
        return None
    R = abs(entry - sl)
    if R <= 0:
        return None
    vals = [abs(tp - entry) / R for tp in tps[:5]]
    return sum(vals) / Decimal(len(vals)) if vals else None


def _get_stop_from_position(p: dict) -> Optional[Decimal]:
    sl = p.get("stopLoss")
    try:
        return Decimal(str(sl)) if sl not in (None, "", "0") else None
    except Exception:
        return None


def _leverage_from_position(p: dict) -> Optional[Decimal]:
    try:
        v = p.get("leverage")
        return Decimal(str(v)) if v not in (None, "", "0") else None
    except Exception:
        return None


def _margin_from_position(p: dict) -> Optional[Decimal]:
    try:
        im = p.get("positionIM")
        if im not in (None, "", "0"):
            return Decimal(str(im))
    except Exception:
        pass
    try:
        value = Decimal(str(p.get("positionValue", "0")))
        lev = _leverage_from_position(p) or Decimal("0")
        if value > 0 and lev > 0:
            return value / lev
    except Exception:
        pass
    return None


def _risk_from_snapshot(
    entry: Decimal,
    sl: Optional[Decimal],
    size: Decimal,
    avg_rr: Optional[Decimal],
) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    Returns (risk_per_unit, risk_usd, potential_reward_usd) as Decimals or None.
    risk_per_unit = |entry - sl|
    risk_usd       = risk_per_unit * size
    potential      = risk_usd * avg_rr (if avg_rr not None)
    """
    if sl is None or entry == sl or size <= 0:
        return None, None, None
    risk_per_unit = abs(entry - sl)
    risk_usd = risk_per_unit * size
    if avg_rr is not None:
        potential_reward = risk_usd * avg_rr
    else:
        potential_reward = None
    return risk_per_unit, risk_usd, potential_reward

# ---------- trade rating ----------

def _rate_trade(
    *,
    result: str,
    realized_rr: Optional[Decimal],
    risk_usd: Optional[Decimal],
    duration_ms: int,
    num_adds: int,
    num_partials: int,
) -> Tuple[int, str]:
    """
    Rate a trade 1–10 based on:
      - realized_rr (primary)
      - result (WIN/LOSS/BREAKEVEN)
      - duration (ultra-short vs more "patient")
      - structural noise (adds/partials count)

    This is not "holy truth", it's a heuristic score so AI & human can
    quickly spot A+ vs trash trades.
    """
    # Base from result
    base = 5.0
    res = (result or "UNKNOWN").upper()

    # RR contribution
    rr_val: Optional[float] = None
    if realized_rr is not None:
        try:
            rr_val = float(realized_rr)
        except Exception:
            rr_val = None

    if rr_val is not None:
        if rr_val >= 3.0:
            base = 9.0
        elif rr_val >= 2.0:
            base = 8.0
        elif rr_val >= 1.0:
            base = 7.0
        elif rr_val >= 0.5:
            base = 6.0
        elif rr_val >= 0.0:
            base = 5.0
        elif rr_val > -0.5:
            base = 4.0
        elif rr_val > -1.0:
            base = 3.0
        elif rr_val > -2.0:
            base = 2.0
        else:
            base = 1.0
    else:
        # Fallback if RR missing but we know result
        if res == "WIN":
            base = 7.0
        elif res == "LOSS":
            base = 4.0
        elif res == "BREAKEVEN":
            base = 5.0

    # Duration adjustment
    # < 30s: likely noise scalp -> slight penalty
    # > 2h with positive RR: small bonus
    dur_s = duration_ms / 1000.0
    if dur_s < 30:
        base -= 0.5
    elif dur_s > 7200 and rr_val is not None and rr_val >= 2.0:
        base += 0.5

    # Adds/partials: too much chopping reduces rating a bit
    churn = num_adds + num_partials
    if churn > 4:
        base -= min(2.0, 0.25 * (churn - 4))

    # Clamp and round
    score = int(max(1, min(10, round(base))))

    # Build reason text
    reasons: List[str] = []
    if rr_val is not None:
        reasons.append(f"RR≈{rr_val:.2f}")
    reasons.append(f"result={res}")
    reasons.append(f"duration={int(dur_s)}s")
    reasons.append(f"adds={num_adds},partials={num_partials}")
    if risk_usd is not None:
        try:
            reasons.append(f"risk≈{float(risk_usd):.2f}usd")
        except Exception:
            pass

    return score, "; ".join(reasons)

# ---------- executions stream ----------

def _fetch_exec_batch(cursor: Optional[str]) -> Tuple[List[dict], Optional[str]]:
    params = {"category": CATEGORY, "limit": "200"}
    if cursor:
        params["cursor"] = cursor
    r = bybit_get("/v5/execution/list", params)
    result = (r.get("result", {}) or {})
    rows = result.get("list", []) or []
    next_cur = result.get("nextPageCursor") or None
    return rows, next_cur


def _exec_is_trade(e: dict) -> bool:
    # Filter to true trade executions
    try:
        return (e.get("execType") == "Trade") and Decimal(str(e.get("execQty", "0"))) > 0
    except Exception:
        return False


def _side_from_exec(e: dict) -> Optional[str]:
    s = e.get("side")
    if s in ("Buy", "Sell"):
        return s
    return None


def _entry_order_meta(e: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract order type and liquidity from an execution row, if present.
    """
    otype = e.get("orderType") or None
    liq = None
    if "isMaker" in e:
        try:
            is_maker = str(e.get("isMaker")).lower() == "true"
            liq = "MAKER" if is_maker else "TAKER"
        except Exception:
            liq = None
    return otype, liq

# ---------- helpers for final pnl ----------

def _closed_pnl_latest(symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    try:
        r = bybit_get("/v5/position/closed-pnl", {"category": CATEGORY, "symbol": symbol, "limit": "1"})
        rows = (r.get("result", {}) or {}).get("list", []) or []
        if not rows:
            return None, None
        row = rows[0]
        pnl = Decimal(str(row.get("closedPnl", "0")))
        exit_px = row.get("avgExitPrice")
        exit_px = Decimal(str(exit_px)) if exit_px not in (None, "", "0") else None
        return pnl, exit_px
    except Exception:
        return None, None

# ---------- main loop ----------

def loop():
    tg.info("📝📒 Flashback Trade Journal v3 started.")
    open_state: Dict[str, dict] = _load_open_state()
    cursor = _load_cursor()

    def _pos_map() -> Dict[str, dict]:
        m: Dict[str, dict] = {}
        for p in list_open_positions():
            try:
                sym = p["symbol"]
                m[sym] = p
            except Exception:
                continue
        return m

    pos_now = _pos_map()

    # On boot, announce any already-open positions (best-effort snapshot)
    for sym, p in pos_now.items():
        if sym not in open_state:
            entry = Decimal(str(p["avgPrice"]))
            size  = Decimal(str(p["size"]))
            side  = p["side"]
            direction = _direction_from_side(side)
            sl    = _get_stop_from_position(p)
            sl_f, tps_f = _kline_infer_grid(sym, side, entry)
            if sl is None:
                sl = sl_f
            rr = _avg_rr(entry, sl, tps_f)
            eq_open = get_equity_usdt()
            risk_per_unit, risk_usd, potential_reward = _risk_from_snapshot(entry, sl, size, rr)

            ts_open = _now_ms()
            snap = {
                "ts_open": ts_open,
                "ts_open_iso": _to_iso(ts_open),
                "symbol": sym,
                "side": side,
                "direction": direction,
                "entry_price": str(entry),
                "size": str(size),
                "entry_notional_usd": _fmt_dec(entry * size, places=2),
                "leverage": str(_leverage_from_position(p)) if _leverage_from_position(p) is not None else None,
                "init_margin": str(_margin_from_position(p)) if _margin_from_position(p) is not None else None,
                "stop_loss": str(sl) if sl is not None else None,
                "tp_prices": [str(x) for x in tps_f],
                "avg_rr_5": str(rr) if rr is not None else None,
                "risk_per_unit": _fmt_dec(risk_per_unit, places=4),
                "risk_usd": _fmt_dec(risk_usd, places=2),
                "potential_reward_usd": _fmt_dec(potential_reward, places=2),
                "equity_at_open": _fmt_dec(eq_open, places=2),
                "entry_order_type": None,
                "entry_liquidity": None,
                "num_adds": 0,
                "num_partials": 0,
                "account": ACCOUNT_LABEL,
                "journal_version": JOURNAL_VERSION,
            }
            open_state[sym] = snap
            _save_open_state(open_state)

            tg.trade(
                f"🟢🚀 NEW TRADE | {sym} {direction} @ {snap['entry_price']} "
                f"📏 size {snap['size']} | ⚙️ lev {snap.get('leverage')} "
                f"🛡 SL {snap.get('stop_loss')} | 💰 R {snap.get('risk_usd')} usd "
                f"📊 RR~{snap.get('avg_rr_5')} | 🎯 POT {snap.get('potential_reward_usd')} usd "
                f"🎯 TP {', '.join(snap['tp_prices'])}"
            )

    _save_open_state(open_state)

    while True:
        try:
            # 1) Pull a batch of executions
            rows, next_cur = _fetch_exec_batch(cursor)
            # Bybit returns newest-first; we want chronological
            rows.reverse()

            # 2) Refresh current positions
            pos_now = _pos_map()

            # 3) Process executions to generate entry/add/partial pings
            for e in rows:
                if not _exec_is_trade(e):
                    continue

                symbol = e.get("symbol")
                side_exec = _side_from_exec(e)
                if not symbol or not side_exec:
                    continue

                qty  = Decimal(str(e.get("execQty", "0")))
                px   = Decimal(str(e.get("execPrice", "0")))
                ts   = int(str(e.get("execTime", _now_ms())))
                pos  = pos_now.get(symbol)

                # If there is a current position, get its side and size
                pos_side = pos.get("side") if pos else None
                pos_size = Decimal(str(pos.get("size", "0"))) if pos else Decimal("0")

                # Determine whether this execution is adding or reducing
                is_entry_or_add = (pos_side == side_exec) or (pos is None and side_exec in ("Buy", "Sell"))
                is_exit_partial = (pos is not None) and (pos_side is not None) and (
                    (pos_side == "Buy" and side_exec == "Sell") or
                    (pos_side == "Sell" and side_exec == "Buy")
                )

                # 3a) ENTRY or ADD
                if is_entry_or_add:
                    snap = open_state.get(symbol)
                    # New symbol or side changed: create/refresh snapshot
                    if snap is None or snap.get("side") != pos_side:
                        # Build from current position snapshot if available
                        if pos:
                            entry = Decimal(str(pos["avgPrice"]))
                            size  = Decimal(str(pos["size"]))
                            side_now = pos["side"]
                            direction = _direction_from_side(side_now)
                            sl = _get_stop_from_position(pos)
                            sl_f, tps_f = _kline_infer_grid(symbol, side_now, entry)
                            if sl is None:
                                sl = sl_f
                            rr = _avg_rr(entry, sl, tps_f)
                            eq_open = get_equity_usdt()
                            risk_per_unit, risk_usd, potential_reward = _risk_from_snapshot(entry, sl, size, rr)
                            order_type, liquidity = _entry_order_meta(e)

                            ts_open = ts or _now_ms()
                            snap = {
                                "ts_open": ts_open,
                                "ts_open_iso": _to_iso(ts_open),
                                "symbol": symbol,
                                "side": side_now,
                                "direction": direction,
                                "entry_price": str(entry),
                                "size": str(size),
                                "entry_notional_usd": _fmt_dec(entry * size, places=2),
                                "leverage": str(_leverage_from_position(pos)) if _leverage_from_position(pos) is not None else None,
                                "init_margin": str(_margin_from_position(pos)) if _margin_from_position(pos) is not None else None,
                                "stop_loss": str(sl) if sl is not None else None,
                                "tp_prices": [str(x) for x in tps_f],
                                "avg_rr_5": str(rr) if rr is not None else None,
                                "risk_per_unit": _fmt_dec(risk_per_unit, places=4),
                                "risk_usd": _fmt_dec(risk_usd, places=2),
                                "potential_reward_usd": _fmt_dec(potential_reward, places=2),
                                "equity_at_open": _fmt_dec(eq_open, places=2),
                                "entry_order_type": order_type,
                                "entry_liquidity": liquidity,
                                "num_adds": 0,
                                "num_partials": 0,
                                "account": ACCOUNT_LABEL,
                                "journal_version": JOURNAL_VERSION,
                            }
                            open_state[symbol] = snap
                            _save_open_state(open_state)

                            tg.trade(
                                f"🟢🚀 NEW TRADE | {symbol} {direction} @ {snap['entry_price']} "
                                f"📏 size {snap['size']} | ⚙️ lev {snap.get('leverage')} "
                                f"🎛 {order_type or 'order'} | 🔁 {liquidity or 'liq?'} "
                                f"🛡 SL {snap.get('stop_loss')} | 💰 R {snap.get('risk_usd')} usd "
                                f"📊 RR~{snap.get('avg_rr_5')} | 🎯 POT {snap.get('potential_reward_usd')} usd "
                                f"🎯 TP {', '.join(snap['tp_prices'])}"
                            )
                        else:
                            # No position yet (race), still emit a lightweight ADD/ENTRY
                            tg.trade(f"🟢✨ ENTRY {symbol} {side_exec} fill {qty} @ {px}")
                    else:
                        # It’s an add-on to an existing position
                        snap = open_state.get(symbol, {})
                        adds = int(snap.get("num_adds", 0)) + 1
                        snap["num_adds"] = adds
                        open_state[symbol] = snap
                        _save_open_state(open_state)

                        tg.trade(
                            f"➕📈 ADD {symbol} {side_exec} fill {qty} @ {px} "
                            f"| 📏 size now ≈ {pos_size} | 🔁 adds {adds}"
                        )

                # 3b) PARTIAL EXIT
                if is_exit_partial:
                    snap = open_state.get(symbol, {})
                    partials = int(snap.get("num_partials", 0)) + 1
                    snap["num_partials"] = partials
                    open_state[symbol] = snap
                    _save_open_state(open_state)

                    # Remaining size from current position map is authoritative
                    tg.trade(
                        f"➖📉 PARTIAL EXIT {symbol} {side_exec} fill {qty} @ {px} "
                        f"| 📏 remaining ≈ {pos_size} | ✂️ partials {partials}"
                    )

            # 4) Detect full closures via positions diff and write ledger rows
            cur_syms = set(pos_now.keys())
            tracked_syms = list(open_state.keys())
            for sym in tracked_syms:
                if sym not in cur_syms:
                    # Position fully closed -> fetch closed-PnL and finalize
                    open_row = open_state.get(sym) or {}
                    pnl, exit_px = _closed_pnl_latest(sym)
                    now_ms = _now_ms()
                    dur = now_ms - int(open_row.get("ts_open", now_ms))

                    # Parse risk_usd back to Decimal if present
                    risk_usd_dec: Optional[Decimal] = None
                    try:
                        if open_row.get("risk_usd") is not None:
                            risk_usd_dec = Decimal(str(open_row["risk_usd"]))
                    except Exception:
                        risk_usd_dec = None

                    realized_rr: Optional[Decimal] = None
                    if pnl is not None and risk_usd_dec is not None and risk_usd_dec > 0:
                        realized_rr = pnl / risk_usd_dec

                    if pnl is None:
                        result = "UNKNOWN"
                    elif pnl > 0:
                        result = "WIN"
                    elif pnl < 0:
                        result = "LOSS"
                    else:
                        result = "BREAKEVEN"

                    eq_after = get_equity_usdt()

                    num_adds = int(open_row.get("num_adds", 0) or 0)
                    num_partials = int(open_row.get("num_partials", 0) or 0)

                    # Compute rating
                    rating_score, rating_reason = _rate_trade(
                        result=result,
                        realized_rr=realized_rr,
                        risk_usd=risk_usd_dec,
                        duration_ms=dur,
                        num_adds=num_adds,
                        num_partials=num_partials,
                    )

                    row: Dict[str, Any] = {
                        **open_row,
                        "ts_close": now_ms,
                        "ts_close_iso": _to_iso(now_ms),
                        "duration_ms": dur,
                        "duration_human": _human_dur(dur),
                        "realized_pnl": str(pnl) if pnl is not None else None,
                        "realized_rr": _fmt_dec(realized_rr, places=2) if realized_rr is not None else None,
                        "result": result,
                        "exit_price": str(exit_px) if exit_px is not None else None,
                        "equity_after_close": _fmt_dec(eq_after, places=2),
                        "symbol": sym,
                        "account": open_row.get("account", ACCOUNT_LABEL),
                        "journal_version": open_row.get("journal_version", JOURNAL_VERSION),
                        "rating_score": rating_score,
                        "rating_reason": rating_reason,
                    }
                    _write_jsonl(row)

                    # Pretty close summary
                    emoji_result = "✅💰" if result == "WIN" else "❌💸" if result == "LOSS" else "⚪️😐"
                    msg = (
                        "{flag} CLOSE {sym} {direction} | 💵 PnL {pnl} usd "
                        "| 📊 RR {rr} | ⏱ {dur} | 🏦 eq {eq_open}→{eq_after} "
                        "| ➕ adds {adds} | ✂️ partials {partials} "
                        "| ⭐ rating {rating}/10"
                    ).format(
                        flag=f"🔴🏁{emoji_result}",
                        sym=sym,
                        direction=row.get("direction", row.get("side", "?")),
                        pnl=row.get("realized_pnl"),
                        rr=row.get("realized_rr"),
                        dur=row.get("duration_human"),
                        eq_open=row.get("equity_at_open"),
                        eq_after=row.get("equity_after_close"),
                        adds=num_adds,
                        partials=num_partials,
                        rating=rating_score,
                    )
                    tg.trade(msg)

                    open_state.pop(sym, None)
                    _save_open_state(open_state)

            # 5) Advance cursor and sleep
            if next_cur is not None:
                cursor = next_cur
                _save_cursor(cursor)

            time.sleep(POLL_SECONDS)

        except Exception as e:
            tb = traceback.format_exc()
            tg.error(f"[Journal] {e}\n{tb}")
            time.sleep(5)


if __name__ == "__main__":
    loop()
