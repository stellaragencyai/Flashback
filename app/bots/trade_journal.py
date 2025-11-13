# app/bots/trade_journal.py
# Flashback — Trade Journal (Main)
#
# What it does (properly now)
# - Streams executions with a persistent cursor so we don't miss partials.
# - Classifies each new fill as ENTRY/ADD or PARTIAL EXIT based on current position side.
# - On first entry per symbol: captures an OPEN snapshot (entry, size, lev, margin, SL, TP grid).
# - On partial exits: sends PnL-ish updates (based on execs) and remaining size from positions.
# - On full close (size -> 0): fetches closed PnL, composes a final trade summary, appends JSONL.
#
# Assumptions
# - Linear USDT perps, Unified, CROSS. Keys and Telegram set in .env.
# - flashback_common has time-sync + retry patch and Telegram helper.
#
# Files
# - app/state/journal.jsonl         (append-only ledger of closed trades)
# - app/state/journal_cursor.json   (stores Bybit executions cursor)
# - app/state/journal_open.json     (last known open snapshot per symbol)

import time
from decimal import Decimal
from pathlib import Path
from typing import Dict, Tuple, List, Optional
import orjson

from app.core.flashback_common import (
    bybit_get, send_tg, list_open_positions, get_equity_usdt,
    last_price, get_ticks, psnap, atr14
)

CATEGORY = "linear"
POLL_SECONDS = 3

JOURNAL_LEDGER = Path("app/state/journal.jsonl")
CURSOR_PATH    = Path("app/state/journal_cursor.json")
OPEN_STATE     = Path("app/state/journal_open.json")

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

def _write_jsonl(row: dict) -> None:
    JOURNAL_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with JOURNAL_LEDGER.open("ab") as f:
        f.write(orjson.dumps(row) + b"\n")

def _load_cursor() -> Optional[str]:
    try:
        return orjson.loads(CURSOR_PATH.read_bytes()).get("cursor")
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
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

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
    return (e.get("execType") == "Trade") and Decimal(str(e.get("execQty", "0"))) > 0

def _side_from_exec(e: dict) -> Optional[str]:
    s = e.get("side")
    if s in ("Buy", "Sell"):
        return s
    return None

# ---------- main loop ----------

def loop():
    send_tg("📝 Flashback Trade Journal started.")
    open_state: Dict[str, dict] = _load_open_state()
    cursor = _load_cursor()

    # prime current positions snapshot
    def _pos_map():
        m = {}
        for p in list_open_positions():
            m[p["symbol"]] = p
        return m

    pos_now = _pos_map()

    # On boot, announce any already-open positions
    for sym, p in pos_now.items():
        if sym not in open_state:
            entry = Decimal(str(p["avgPrice"]))
            size  = Decimal(str(p["size"]))
            side  = p["side"]
            sl    = _get_stop_from_position(p)
            sl_f, tps_f = _kline_infer_grid(sym, side, entry)
            if sl is None:
                sl = sl_f
            rr = _avg_rr(entry, sl, tps_f)
            snap = {
                "ts_open": _now_ms(),
                "symbol": sym,
                "side": side,
                "entry_price": str(entry),
                "size": str(size),
                "leverage": str(_leverage_from_position(p)) if _leverage_from_position(p) is not None else None,
                "init_margin": str(_margin_from_position(p)) if _margin_from_position(p) is not None else None,
                "stop_loss": str(sl) if sl is not None else None,
                "tp_prices": [str(x) for x in tps_f],
                "avg_rr_5": str(rr) if rr is not None else None,
            }
            open_state[sym] = snap
            send_tg(
                f"🟢 OPEN {sym} {side} @ {snap['entry_price']} "
                f"| lev {snap.get('leverage')} | size {snap['size']} "
                f"| SL {snap.get('stop_loss')} | TP {', '.join(snap['tp_prices'])}"
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
                            sl = _get_stop_from_position(pos)
                            sl_f, tps_f = _kline_infer_grid(symbol, side_now, entry)
                            if sl is None:
                                sl = sl_f
                            rr = _avg_rr(entry, sl, tps_f)
                            snap = {
                                "ts_open": ts or _now_ms(),
                                "symbol": symbol,
                                "side": side_now,
                                "entry_price": str(entry),
                                "size": str(size),
                                "leverage": str(_leverage_from_position(pos)) if _leverage_from_position(pos) is not None else None,
                                "init_margin": str(_margin_from_position(pos)) if _margin_from_position(pos) is not None else None,
                                "stop_loss": str(sl) if sl is not None else None,
                                "tp_prices": [str(x) for x in tps_f],
                                "avg_rr_5": str(rr) if rr is not None else None,
                            }
                            open_state[symbol] = snap
                            send_tg(
                                f"🟢 OPEN {symbol} {side_now} @ {snap['entry_price']} "
                                f"| lev {snap.get('leverage')} | size {snap['size']} "
                                f"| SL {snap.get('stop_loss')} | TP {', '.join(snap['tp_prices'])}"
                            )
                        else:
                            # No position yet (race), still emit a lightweight ADD/ENTRY
                            send_tg(f"🟢 ENTRY {symbol} {side_exec} fill {qty} @ {px}")
                    else:
                        # It’s an add-on to an existing position
                        send_tg(f"➕ ADD {symbol} {side_exec} fill {qty} @ {px} | size now ≈ {pos_size}")

                # 3b) PARTIAL EXIT
                if is_exit_partial:
                    # Remaining size from current position map is authoritative
                    send_tg(f"➖ PARTIAL EXIT {symbol} {side_exec} fill {qty} @ {px} | remaining ≈ {pos_size}")

            # 4) Detect full closures via positions diff and write ledger rows
            # Build set of all seen symbols (open_state keys + current pos keys)
            cur_syms = set(pos_now.keys())
            tracked_syms = list(open_state.keys())
            for sym in tracked_syms:
                if sym not in cur_syms:
                    # Position fully closed -> fetch closed-PnL and finalize
                    open_row = open_state.get(sym) or {}
                    pnl, exit_px = _closed_pnl_latest(sym)
                    now = _now_ms()
                    dur = now - int(open_row.get("ts_open", now))
                    row = {
                        **open_row,
                        "ts_close": now,
                        "duration_ms": dur,
                        "duration_human": _human_dur(dur),
                        "realized_pnl": str(pnl) if pnl is not None else None,
                        "exit_price": str(exit_px) if exit_px is not None else None,
                        "equity_after_close": str(get_equity_usdt()),
                        "symbol": sym,
                    }
                    _write_jsonl(row)
                    send_tg(
                        "🔴 CLOSE {sym} | PnL {pnl} | exit {exit_px} | dur {dur} | eq {eq}".format(
                            sym=sym,
                            pnl=row.get("realized_pnl"),
                            exit_px=row.get("exit_price"),
                            dur=row.get("duration_human"),
                            eq=row.get("equity_after_close"),
                        )
                    )
                    open_state.pop(sym, None)
                    _save_open_state(open_state)

            # 5) Advance cursor and sleep
            if next_cur:
                cursor = next_cur
                _save_cursor(cursor)
            time.sleep(POLL_SECONDS)

        except Exception as e:
            send_tg(f"[Journal] {e}")
            time.sleep(5)

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

if __name__ == "__main__":
    loop()
