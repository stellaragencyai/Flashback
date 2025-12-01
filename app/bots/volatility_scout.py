# app/bots/volatility_scout.py
# Flashback — Volatility Scout (Main)
#
# Purpose:
#   Continuously scan Bybit linear USDT perps and flag sudden, tradable volatility.
#   Triggers:
#     A) ±X% move over Y seconds (config via env: SCOUT_MOVE_PCT, SCOUT_WINDOW_SEC)
#     B) Volume pop: last 1m volume > VOL_MULT * 20-bar avg
#     C) Micro-ATR surge: ATR(14, 1m) > ATR_MULT * 20-bar median ATR
#
# Telegram alert includes: symbol, change %, latest price, bias hint,
# and which trigger(s) fired. It rate-limits duplicate pings per symbol+trigger.
#
# Notes:
#   - Uses public market endpoints (no auth) for speed.
#   - Polls every SCOUT_POLL_SEC.
#   - Keeps a tiny in-memory debounce so you don't get spammed.

import os
import time
import math
import statistics
from decimal import Decimal
from typing import Dict, List, Tuple

from app.core.flashback_common import (
    bybit_get, send_tg, list_linear_usdt_symbols
)

# ---- Tunables (env-overridable) ----
SCAN_SECONDS      = int(os.getenv("SCOUT_POLL_SEC", "10"))          # how often to scan universe
MOVE_PCT_THRESH   = Decimal(os.getenv("SCOUT_MOVE_PCT", "20"))      # default ±20%
WINDOW_SECONDS    = int(os.getenv("SCOUT_WINDOW_SEC", "180"))       # default 180s (3 minutes)
VOL_MULT          = Decimal(os.getenv("SCOUT_VOL_MULT", "3"))       # last vol > 3x avg(20)
ATR_MULT          = Decimal(os.getenv("SCOUT_ATR_MULT", "2"))       # last ATR14 > 2x median of last 20 ATRs
MAX_SYMBOLS       = int(os.getenv("SCOUT_MAX_SYMBOLS", "800"))      # safety cap

# Debounce windows (seconds) before we alert again on the SAME trigger
DEBOUNCE_PRICE = int(os.getenv("SCOUT_DEBOUNCE_PRICE", "300"))      # default 5 min
DEBOUNCE_VOL   = int(os.getenv("SCOUT_DEBOUNCE_VOL", "600"))        # default 10 min
DEBOUNCE_ATR   = int(os.getenv("SCOUT_DEBOUNCE_ATR", "600"))        # default 10 min

CATEGORY = "linear"

def _kline_1m(symbol: str, limit: int = 40) -> List[List[str]]:
    """
    Return last 'limit' 1-minute klines (ascending time).
    Schema per row: [startTime, open, high, low, close, volume, turnover]
    """
    r = bybit_get("/v5/market/kline",
                  {"category": CATEGORY, "symbol": symbol, "interval": "1", "limit": str(limit)},
                  auth=False)
    rows = r.get("result", {}).get("list", []) or []
    rows.reverse()
    return rows

def _pct_change(a: Decimal, b: Decimal) -> Decimal:
    if a <= 0:
        return Decimal("0")
    return (b - a) * Decimal("100") / a

def _atr14_from_1m(rows: List[List[str]]) -> Decimal:
    """
    Simple ATR(14) on 1m bars from kline rows (already ascending).
    """
    if len(rows) < 15:
        return Decimal("0")
    trs = []
    prev_close = Decimal(rows[0][4])
    for i in range(1, len(rows)):
        high = Decimal(rows[i][2])
        low  = Decimal(rows[i][3])
        close= Decimal(rows[i][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    if len(trs) < 14:
        return Decimal("0")
    return sum(trs[-14:]) / Decimal(14)

def _median(vals: List[Decimal]) -> Decimal:
    if not vals:
        return Decimal("0")
    return Decimal(str(statistics.median([float(v) for v in vals])))

def _volatility_signals(symbol: str) -> Tuple[bool, bool, bool, Dict[str, Decimal]]:
    """
    Returns (hit_price_window, hit_vol_spike, hit_atr_surge, metrics)
    metrics includes: pct_window, last_vol, avg20_vol, last_atr, med20_atr, px
    """
    # need enough rows for our lookback window + ATR/vol calcs
    bars_back = max(1, math.ceil(WINDOW_SECONDS / 60))
    need = max(21 + bars_back, 40)  # ensure room for 20 vol bars and ATR series
    rows = _kline_1m(symbol, limit=need)
    if len(rows) < (bars_back + 1):
        return False, False, False, {"pct_window": Decimal("0")}

    # Price change over WINDOW_SECONDS using 1m closes: t-bars_back to t-now
    close_then = Decimal(rows[-(bars_back + 1)][4])
    close_now  = Decimal(rows[-1][4])
    pct_window = _pct_change(close_then, close_now)
    hit_price  = abs(pct_window) >= MOVE_PCT_THRESH

    # Volume spike: last vol vs avg of last 20 (excluding current)
    if len(rows) >= 22:
        vols = [Decimal(r[5]) for r in rows[-21:-1]]  # previous 20
    else:
        vols = []
    last_vol = Decimal(rows[-1][5])
    avg20    = (sum(vols) / Decimal(len(vols))) if vols else Decimal("0")
    hit_vol  = avg20 > 0 and last_vol >= (avg20 * VOL_MULT)

    # ATR surge: compare last ATR vs median of previous ~20 ATRs
    atr_series: List[Decimal] = []
    for end in range(15, len(rows) + 1):
        atr_series.append(_atr14_from_1m(rows[:end]))
    if len(atr_series) < 2:
        hit_atr = False
        last_atr = Decimal("0")
        med20 = Decimal("0")
    else:
        last_atr = atr_series[-1]
        hist = atr_series[:-1]
        med20 = _median(hist[-20:])
        hit_atr = med20 > 0 and last_atr >= (med20 * ATR_MULT)

    return hit_price, hit_vol, hit_atr, {
        "pct_window": pct_window,
        "last_vol": last_vol,
        "avg20_vol": avg20,
        "last_atr": last_atr,
        "med20_atr": med20,
        "px": close_now
    }

def _bias_from_change(pct_window: Decimal) -> str:
    if pct_window >= MOVE_PCT_THRESH:
        return "LONG bias (momentum)"
    if pct_window <= -MOVE_PCT_THRESH:
        return "SHORT bias (momentum)"
    return "Neutral bias (confirm structure)"

def loop():
    send_tg(f"📡 Volatility Scout online: alert at ±{MOVE_PCT_THRESH}% / {WINDOW_SECONDS}s")
    # debounce maps: symbol -> last alert timestamp per trigger
    last_ping_price: Dict[str, float] = {}
    last_ping_vol:   Dict[str, float] = {}
    last_ping_atr:   Dict[str, float] = {}

    # Cache symbol list once per hour
    symbols = list_linear_usdt_symbols()[:MAX_SYMBOLS]
    last_universe_refresh = time.time()

    while True:
        try:
            now = time.time()
            # Periodic universe refresh
            if now - last_universe_refresh > 3600:
                try:
                    symbols = list_linear_usdt_symbols()[:MAX_SYMBOLS]
                except Exception:
                    pass
                last_universe_refresh = now

            for sym in symbols:
                try:
                    hit_price, hit_vol, hit_atr, m = _volatility_signals(sym)
                except Exception:
                    continue  # skip symbol on data hiccup

                # 1) Price explosion/implosion over WINDOW_SECONDS
                if hit_price:
                    last = last_ping_price.get(sym, 0)
                    if now - last >= DEBOUNCE_PRICE:
                        bias = _bias_from_change(m["pct_window"])
                        send_tg(
                            f"⚡ {sym} move {m['pct_window']:.2f}% in {WINDOW_SECONDS}s | px {m['px']} | {bias}"
                        )
                        last_ping_price[sym] = now

                # 2) Volume pop
                if hit_vol:
                    last = last_ping_vol.get(sym, 0)
                    if now - last >= DEBOUNCE_VOL:
                        send_tg(
                            f"📈 Volume spike {sym}: last={m['last_vol']:.0f} vs avg20={m['avg20_vol']:.0f}"
                        )
                        last_ping_vol[sym] = now

                # 3) ATR micro-surge
                if hit_atr:
                    last = last_ping_atr.get(sym, 0)
                    if now - last >= DEBOUNCE_ATR:
                        send_tg(
                            f"🔥 ATR surge {sym}: last={m['last_atr']:.6f} vs med20={m['med20_atr']:.6f}"
                        )
                        last_ping_atr[sym] = now

            time.sleep(SCAN_SECONDS)

        except Exception as e:
            try:
                send_tg(f"[VolScout] {e}")
            except Exception:
                pass
            time.sleep(5)

if __name__ == "__main__":
    loop()
