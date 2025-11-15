#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback / Base44 — Signal Engine v1 (AI-logging enabled)

What this does (MVP version):
- Loads config from .env (symbols, timeframes, polling/heartbeat).
- Uses Bybit public kline endpoint to fetch recent candles.
- For each (symbol, timeframe), decides a very simple LONG/SHORT bias:
    • LONG  if last close > previous close AND last close > simple MA(last N closes)
    • SHORT if last close < previous close AND last close < simple MA(last N closes)
    • Otherwise: no signal.
- Emits at most ONE signal per closed bar per (symbol, timeframe).
- Sends human-readable alerts via the main Telegram notifier.
- Logs every signal into the AI event store via app.core.ai_hooks.log_signal_from_engine.

This is intentionally simple logic. The important part is:
  → Every time the engine emits a signal, it is recorded in state/ai_store.db

.env keys used (all optional with defaults):
    BYBIT_BASE              = https://api.bybit.com

    SIG_ENABLED             = true
    SIG_DRY_RUN             = true           # currently unused (no orders placed)
    SIG_SYMBOLS             = BTCUSDT,ETHUSDT
    SIG_TIMEFRAMES          = 5,15
    SIG_POLL_SEC            = 15
    SIG_HEARTBEAT_SEC       = 300

Telegram:
    Uses app.core.notifier_bot.get_notifier("main")

Run from project root:
    python -m app.bots.signal_engine

"""

from __future__ import annotations

import os
import time
import math
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import requests
from dotenv import load_dotenv

# AI logging
from app.core.ai_hooks import log_signal_from_engine

# Telegram notifier (same pattern as supervisor)
from app.core.notifier_bot import get_notifier


# ---------- Paths & Env ----------

THIS_FILE = Path(__file__).resolve()
BOTS_DIR = THIS_FILE.parent             # .../app/bots
APP_DIR = BOTS_DIR.parent               # .../app
ROOT_DIR = APP_DIR.parent               # project root

# Force project root as working directory so relative paths are sane
os.chdir(ROOT_DIR)

# Load .env from project root
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")

def _parse_bool(val: Optional[str], default: bool) -> bool:
    if val is None:
        return default
    v = val.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default

SIG_ENABLED = _parse_bool(os.getenv("SIG_ENABLED"), True)
SIG_DRY_RUN = _parse_bool(os.getenv("SIG_DRY_RUN"), True)

_raw_symbols = os.getenv("SIG_SYMBOLS", "BTCUSDT,ETHUSDT")
SIG_SYMBOLS: List[str] = [s.strip().upper() for s in _raw_symbols.split(",") if s.strip()]

_raw_tfs = os.getenv("SIG_TIMEFRAMES", "5,15")
SIG_TIMEFRAMES: List[str] = [tf.strip() for tf in _raw_tfs.split(",") if tf.strip()]

SIG_POLL_SEC = int(os.getenv("SIG_POLL_SEC", "15"))
SIG_HEARTBEAT_SEC = int(os.getenv("SIG_HEARTBEAT_SEC", "300"))

# How many closes to use for our toy MA logic
MA_LOOKBACK = 8


# ---------- Telegram Notifier ----------

tg = get_notifier("main")


def tg_info(msg: str) -> None:
    try:
        tg.info(msg)
    except Exception:
        print(f"[signal_engine][TG error] {msg}")


def tg_error(msg: str) -> None:
    try:
        tg.error(msg)
    except Exception:
        print(f"[signal_engine][TG error] {msg}")


# ---------- Bybit Market Data ----------

def fetch_recent_klines(
    symbol: str,
    interval: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Fetch recent kline data from Bybit v5 public endpoint.

    Returns a list of dicts, newest LAST, with fields:
        ts_ms, open, high, low, close, volume
    """
    url = f"{BYBIT_BASE}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit),
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit kline error {data.get('retCode')}: {data.get('retMsg')}")

    raw_list = data.get("result", {}).get("list", []) or []

    klines: List[Dict[str, Any]] = []
    # Bybit returns list of lists; we map them to dicts.
    # Format: [startTime, open, high, low, close, volume, turnover]
    for row in raw_list:
        ts_ms = int(row[0])
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])
        v = float(row[5])
        klines.append(
            {
                "ts_ms": ts_ms,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            }
        )

    # Ensure sorted oldest -> newest
    klines.sort(key=lambda x: x["ts_ms"])
    return klines


# ---------- Simple Signal Logic ----------

def compute_simple_signal(candles: List[Dict[str, Any]]) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Given a list of candles (oldest -> newest), return:
        (side, debug_info)

    side: "LONG" / "SHORT" / None

    Logic:
      - Use last 2 candles:
          prev = candles[-2], last = candles[-1]
      - Compute MA over last N closes (min 3, up to MA_LOOKBACK).
      - LONG  if last.close > prev.close and last.close > MA
      - SHORT if last.close < prev.close and last.close < MA
    """
    debug: Dict[str, Any] = {}

    if len(candles) < 3:
        debug["reason"] = "not_enough_candles"
        return None, debug

    last = candles[-1]
    prev = candles[-2]

    closes = [c["close"] for c in candles[-MA_LOOKBACK:]]
    ma = sum(closes) / len(closes)

    debug.update(
        {
            "last_close": last["close"],
            "prev_close": prev["close"],
            "ma": ma,
        }
    )

    side: Optional[str] = None
    if last["close"] > prev["close"] and last["close"] > ma:
        side = "LONG"
        debug["reason"] = "close_up_above_ma"
    elif last["close"] < prev["close"] and last["close"] < ma:
        side = "SHORT"
        debug["reason"] = "close_down_below_ma"
    else:
        debug["reason"] = "no_clear_edge"

    return side, debug


def tf_display(tf: str) -> str:
    """
    Convert raw interval (e.g. "5") to something nicer like "5m".
    """
    if tf.endswith(("m", "h", "d")):
        return tf
    # crude mapping: assume minutes if just a number
    return f"{tf}m"


# ---------- Main Loop ----------

def main() -> None:
    print("=== Flashback Signal Engine v1 (AI-logging enabled) ===")
    print(f"Project root: {ROOT_DIR}")
    print(f"Using .env:   {ENV_PATH} (exists={ENV_PATH.exists()})")
    print(f"Bybit base:   {BYBIT_BASE}")
    print(f"SIG_ENABLED:  {SIG_ENABLED}")
    print(f"SIG_DRY_RUN:  {SIG_DRY_RUN}")
    print(f"Symbols:      {SIG_SYMBOLS}")
    print(f"Timeframes:   {SIG_TIMEFRAMES}")
    print(f"Poll every:   {SIG_POLL_SEC} sec")
    print(f"Heartbeat:    {SIG_HEARTBEAT_SEC} sec")

    if not SIG_ENABLED:
        msg = "⚠️ Signal engine is disabled (SIG_ENABLED=false). Exiting."
        print(msg)
        tg_info(msg)
        return

    tg_info(
        "🚀 Signal Engine v1 started.\n"
        f"Symbols: {', '.join(SIG_SYMBOLS)}\n"
        f"TFs: {', '.join(tf_display(t) for t in SIG_TIMEFRAMES)}\n"
        f"Poll: {SIG_POLL_SEC}s | Heartbeat: {SIG_HEARTBEAT_SEC}s"
    )

    # Keep track of last bar we emitted a signal for per (symbol, timeframe)
    last_signal_bar: Dict[Tuple[str, str], int] = {}

    start_ts = time.time()
    next_heartbeat = start_ts + SIG_HEARTBEAT_SEC

    while True:
        loop_start = time.time()
        total_signals_this_loop = 0

        for symbol in SIG_SYMBOLS:
            for tf in SIG_TIMEFRAMES:
                key = (symbol, tf)

                try:
                    candles = fetch_recent_klines(symbol, tf, limit=max(MA_LOOKBACK + 2, 10))
                except Exception as e:
                    print(f"[WARN] Failed to fetch klines for {symbol} {tf}: {type(e).__name__}: {e}")
                    tg_error(f"⚠️ Signal engine kline error for {symbol} {tf}: {type(e).__name__}")
                    continue

                if len(candles) < 3:
                    print(f"[INFO] Not enough candles yet for {symbol} {tf}")
                    continue

                latest_bar = candles[-1]
                bar_ts = latest_bar["ts_ms"]

                # Only act once per bar for this (symbol, timeframe)
                last_ts = last_signal_bar.get(key)
                if last_ts is not None and bar_ts <= last_ts:
                    # Already processed this bar
                    continue

                side, debug = compute_simple_signal(candles)
                if side is None:
                    # No signal on this bar; still update last processed bar to avoid re-eval
                    last_signal_bar[key] = bar_ts
                    continue

                last_signal_bar[key] = bar_ts
                total_signals_this_loop += 1

                tf_label = tf_display(tf)
                reason = debug.get("reason", "n/a")
                last_c = debug.get("last_close")
                ma = debug.get("ma")

                # Human-readable Telegram alert
                msg = (
                    f"📡 *Signal Engine v1* — {symbol} / {tf_label}\n"
                    f"Side: *{side}*\n"
                    f"Last close: `{last_c}` | MA({len([c['close'] for c in candles[-MA_LOOKBACK:]])}): `{ma}`\n"
                    f"Reason: `{reason}`\n"
                    f"(No orders placed yet, logging only.)"
                )
                tg_info(msg)

                # AI logging hook
                # Note: owner="AUTO_STRATEGY" for now; later we can route by subaccount/role.
                regime_tags = [reason]
                extra = {
                    "engine": "signal_engine_v1",
                    "raw_debug": debug,
                    "tf_raw": tf,
                }

                signal_id = log_signal_from_engine(
                    symbol=symbol,
                    timeframe=tf_label,
                    side=side,
                    source="signal_engine_v1",
                    confidence=None,          # can be wired to a score later
                    stop_hint=None,           # can be wired to ATR-based SL etc.
                    owner="AUTO_STRATEGY",
                    sub_uid=None,
                    strategy_role="GENERIC_SIGNAL_ENGINE",
                    regime_tags=regime_tags,
                    extra=extra,
                )

                print(f"[SIGNAL] {symbol} {tf_label} {side} | signal_id={signal_id} | reason={reason}")

        # Heartbeat
        now = time.time()
        if now >= next_heartbeat:
            uptime_min = int((now - start_ts) / 60)
            hb = (
                f"🩺 Signal Engine heartbeat (v1)\n"
                f"- Uptime: {uptime_min} min\n"
                f"- Symbols: {', '.join(SIG_SYMBOLS)}\n"
                f"- TFs: {', '.join(tf_display(t) for t in SIG_TIMEFRAMES)}\n"
                f"- Last loop signals: {total_signals_this_loop}"
            )
            tg_info(hb)
            next_heartbeat = now + SIG_HEARTBEAT_SEC

        # Sleep until next poll
        elapsed = time.time() - loop_start
        sleep_for = max(1.0, SIG_POLL_SEC - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[EXIT] Signal Engine interrupted by user.")
