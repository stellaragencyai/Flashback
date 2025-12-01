#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback / Base44 — Signal Engine v2 (Strategy-aware + AI-logging + JSONL export)

What this does:
- If SIG_USE_STRATEGIES=true (default) and app.core.strategies is available:
    • Reads config/strategies.yaml via app.core.strategies.
    • Builds a universe:
          (symbol, timeframe) -> list of strategy dicts (per subaccount).
    • Only scans symbols/TFs belonging to at least one sub-strategy.
    • Emits signals tagged with strategy name + sub_uid for each matched strategy.
- Otherwise (fallback mode):
    • Uses SIG_SYMBOLS / SIG_TIMEFRAMES from .env (like v1, generic).

Shared behavior:
- Uses Bybit public kline endpoint to fetch recent candles.
- For each (symbol, timeframe), decides a simple LONG/SHORT bias:
    • LONG  if last close > previous close AND last close > simple MA(last N closes)
    • SHORT if last close < previous close AND last close < simple MA(last N closes)
    • Otherwise: no signal.
- Emits at most ONE signal per closed bar per (symbol, timeframe).
- Sends human-readable alerts via the main Telegram notifier.
- Logs every signal into the AI event store via app.core.ai_hooks.log_signal_from_engine.
- ALSO appends machine-readable signals to signals/observed.jsonl for auto_executor.

.env keys used (all optional with defaults):
    BYBIT_BASE                = https://api.bybit.com

    SIG_ENABLED               = true
    SIG_DRY_RUN               = true           # currently unused (no orders placed)
    SIG_SYMBOLS               = BTCUSDT,ETHUSDT        # fallback-only
    SIG_TIMEFRAMES            = 5,15                   # fallback-only
    SIG_POLL_SEC              = 15
    SIG_HEARTBEAT_SEC         = 300
    SIG_USE_STRATEGIES        = true          # use app.core.strategies universe

Telegram:
    Uses app.core.notifier_bot.get_notifier("main")

Run from project root:
    python -m app.bots.signal_engine
"""

from __future__ import annotations

import os
import time
import json as _jsonlib
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

# ---------- HTTP client ----------

try:
    import requests  # type: ignore
    _HAS_REQUESTS = True
except Exception:
    # Fallback to stdlib if `requests` is not installed
    import urllib.request as _urllib_request  # type: ignore
    import urllib.parse as _urllib_parse      # type: ignore
    import json as _json_fallback
    _HAS_REQUESTS = False

from dotenv import load_dotenv

# ---------- AI logging (optional) ----------

_HAS_AI_HOOKS = False
try:
    from app.core.ai_hooks import log_signal_from_engine as _real_log_signal_from_engine  # type: ignore
    _HAS_AI_HOOKS = True
except Exception:
    # Stub if ai_hooks / ai_store not wired yet
    def _real_log_signal_from_engine(
        *,
        symbol: str,
        timeframe: str,
        side: str,
        source: str,
        confidence: Optional[float],
        stop_hint: Optional[float],
        owner: str,
        sub_uid: Optional[str],
        strategy_role: str,
        regime_tags: List[str],
        extra: Dict[str, Any],
    ) -> Optional[str]:
        print(
            f"[AI_STUB] log_signal_from_engine: {symbol} {timeframe} {side} | "
            f"strategy={strategy_role} sub_uid={sub_uid} (ai_store not available)"
        )
        return None

# Unified alias
log_signal_from_engine = _real_log_signal_from_engine

# ---------- Telegram notifier ----------

from app.core.notifier_bot import get_notifier

# ---------- Strategy registry ----------

try:
    from app.core import strategies as stratreg
    _HAS_STRATEGY_REGISTRY = True
except Exception:
    stratreg = None  # type: ignore
    _HAS_STRATEGY_REGISTRY = False

# ---------- Paths & env ----------

THIS_FILE = Path(__file__).resolve()
BOTS_DIR = THIS_FILE.parent
APP_DIR = BOTS_DIR.parent
ROOT_DIR = APP_DIR.parent

# Force project root
os.chdir(ROOT_DIR)

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
SIG_USE_STRATEGIES = _parse_bool(os.getenv("SIG_USE_STRATEGIES"), True)

_raw_symbols = os.getenv("SIG_SYMBOLS", "BTCUSDT,ETHUSDT")
SIG_SYMBOLS: List[str] = [s.strip().upper() for s in _raw_symbols.split(",") if s.strip()]

_raw_tfs = os.getenv("SIG_TIMEFRAMES", "5,15")
SIG_TIMEFRAMES: List[str] = [tf.strip() for tf in _raw_tfs.split(",") if tf.strip()]

SIG_POLL_SEC = int(os.getenv("SIG_POLL_SEC", "15"))
SIG_HEARTBEAT_SEC = int(os.getenv("SIG_HEARTBEAT_SEC", "300"))

# Simple MA lookback
MA_LOOKBACK = 8

# Where we write executor-friendly signals
SIGNALS_DIR = ROOT_DIR / "signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_PATH = SIGNALS_DIR / "observed.jsonl"

# Telegram main notifier
tg = get_notifier("main")


def tg_info(msg: str) -> None:
    try:
        tg.info(msg)
    except Exception:
        print(f"[signal_engine][TG info fallback] {msg}")


def tg_error(msg: str) -> None:
    try:
        tg.error(msg)
    except Exception:
        print(f"[signal_engine][TG error fallback] {msg}")


# ---------- Bybit Kline Fetcher ----------

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

    if _HAS_REQUESTS:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    else:
        # stdlib fallback
        query = _urllib_parse.urlencode(params)  # type: ignore[name-defined]
        full_url = f"{url}?{query}"
        with _urllib_request.urlopen(full_url, timeout=10) as fh:  # type: ignore[name-defined]
            if hasattr(fh, "getcode"):
                status = fh.getcode()
                if status >= 400:
                    raise RuntimeError(f"HTTP error {status} when fetching {full_url}")
            body = fh.read()
            data = _json_fallback.loads(body.decode("utf-8"))

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


# ---------- JSONL export for auto_executor ----------

def append_signal_jsonl(
    symbol: str,
    side_text: str,
    tf_label: str,
    bar_ts: int,
    reason: str,
    debug: Dict[str, Any],
    sub_uid: Optional[str] = None,
    strategy_name: Optional[str] = None,
) -> None:
    """
    Append a single JSONL line in the format expected by auto_executor.
    """
    if side_text not in ("LONG", "SHORT"):
        return

    side_exec = "Buy" if side_text == "LONG" else "Sell"

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "side": side_exec,
        "timeframe": tf_label,
        "reason": reason,
        "ts_ms": bar_ts,
        "est_rr": 0.25,
        "debug": {
            "engine": "signal_engine_v2",
            "raw_reason": reason,
            "last_close": debug.get("last_close"),
            "prev_close": debug.get("prev_close"),
            "ma": debug.get("ma"),
        },
    }

    if sub_uid is not None:
        payload["sub_uid"] = str(sub_uid)
    if strategy_name is not None:
        payload["strategy_name"] = strategy_name

    try:
        with SIGNALS_PATH.open("a", encoding="utf-8") as f:
            f.write(_jsonlib.dumps(payload, separators=(",", ":"), ensure_ascii=False))
            f.write("\n")
    except Exception as e:
        print(f"[signal_engine] Failed to append to {SIGNALS_PATH}: {e}")


# ---------- Strategy helpers ----------

def _get_strat_attr(s: Any, key: str, default: Any = None) -> Any:
    """
    Helper that works for both:
      - dict-like strategies
      - object-like strategies (e.g. dataclasses / Pydantic models)
    """
    if isinstance(s, dict):
        return s.get(key, default)
    return getattr(s, key, default)


# ---------- Strategy-aware universe ----------

def build_universe() -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """
    Build mapping:
        (symbol, timeframe_raw) -> [strategy_dict, ...]
    When SIG_USE_STRATEGIES is enabled and strategies.yaml is available,
    we use subaccount strategy definitions.
    Otherwise, fall back to SIG_SYMBOLS / SIG_TIMEFRAMES with no strategies.
    """
    universe: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    if SIG_USE_STRATEGIES and _HAS_STRATEGY_REGISTRY:
        try:
            sub_strats = stratreg.all_sub_strategies()
        except Exception as e:
            print(f"[signal_engine] Failed to load strategy config: {e}")
            sub_strats = []

        for s in sub_strats:
            sub_uid_raw = _get_strat_attr(s, "sub_uid", "")
            sub_uid = str(sub_uid_raw).strip()
            if not sub_uid:
                continue

            name_raw = _get_strat_attr(s, "name", None)
            name = str(name_raw) if name_raw is not None else f"sub-{sub_uid}"

            symbols = _get_strat_attr(s, "symbols", []) or []
            tfs = _get_strat_attr(s, "timeframes", []) or []

            if not symbols or not tfs:
                continue

            automation_mode = _get_strat_attr(s, "automation_mode", None)

            for sym in symbols:
                sym_u = str(sym).upper()
                for tf in tfs:
                    tf_str = str(tf).strip()
                    key = (sym_u, tf_str)
                    entry = {
                        "sub_uid": sub_uid,
                        "name": name,
                        "symbols": symbols,
                        "timeframes": tfs,
                        "automation_mode": automation_mode,
                        "raw": s,
                    }
                    universe.setdefault(key, []).append(entry)

    # Fallback if no strategies or disabled
    if not universe:
        for sym in SIG_SYMBOLS:
            for tf in SIG_TIMEFRAMES:
                universe.setdefault((sym, tf), [])

    return universe


# ---------- Main Loop ----------

def main() -> None:
    universe = build_universe()
    all_symbols = sorted({k[0] for k in universe.keys()})
    all_tfs = sorted({k[1] for k in universe.keys()})

    print("=== Flashback Signal Engine v2 (Strategy-aware, AI-logging + JSONL) ===")
    print(f"Project root: {ROOT_DIR}")
    print(f"Using .env:   {ENV_PATH} (exists={ENV_PATH.exists()})")
    print(f"Bybit base:   {BYBIT_BASE}")
    print(f"SIG_ENABLED:  {SIG_ENABLED}")
    print(f"SIG_DRY_RUN:  {SIG_DRY_RUN}")
    print(f"SIG_USE_STRATEGIES: {SIG_USE_STRATEGIES} (has_registry={_HAS_STRATEGY_REGISTRY})")
    print(f"AI logging available: {_HAS_AI_HOOKS}")
    print(f"Universe symbols:    {all_symbols}")
    print(f"Universe timeframes: {all_tfs}")
    print(f"Poll every:          {SIG_POLL_SEC} sec")
    print(f"Heartbeat:           {SIG_HEARTBEAT_SEC} sec")
    print(f"JSONL path:          {SIGNALS_PATH}")

    if not SIG_ENABLED:
        msg = "⚠️ Signal engine is disabled (SIG_ENABLED=false). Exiting."
        print(msg)
        tg_info(msg)
        return

    if not universe:
        msg = "⚠️ Signal engine universe is empty (no symbols/timeframes). Exiting."
        print(msg)
        tg_info(msg)
        return

    # Startup Telegram
    if SIG_USE_STRATEGIES and _HAS_STRATEGY_REGISTRY:
        try:
            strat_objs = stratreg.all_sub_strategies()
            strat_names: List[str] = []
            for s in strat_objs:
                n = _get_strat_attr(s, "name", None)
                if not n:
                    su = _get_strat_attr(s, "sub_uid", None)
                    n = f"sub-{su}"
                strat_names.append(str(n))
        except Exception:
            strat_names = []
        tg_info(
            "🚀 Signal Engine v2 started (strategy-aware).\n"
            f"Symbols: {', '.join(all_symbols)}\n"
            f"TFs: {', '.join(tf_display(t) for t in all_tfs)}\n"
            f"Strategies: {', '.join(strat_names) or 'n/a'}\n"
            f"Poll: {SIG_POLL_SEC}s | Heartbeat: {SIG_HEARTBEAT_SEC}s\n"
            f"AI logging: {'on' if _HAS_AI_HOOKS else 'OFF (stub)'}"
        )
    else:
        tg_info(
            "🚀 Signal Engine v2 started (fallback mode, .env universe).\n"
            f"Symbols: {', '.join(all_symbols)}\n"
            f"TFs: {', '.join(tf_display(t) for t in all_tfs)}\n"
            f"Poll: {SIG_POLL_SEC}s | Heartbeat: {SIG_HEARTBEAT_SEC}s\n"
            f"AI logging: {'on' if _HAS_AI_HOOKS else 'OFF (stub)'}"
        )

    # Track last bar we emitted a signal for
    last_signal_bar: Dict[Tuple[str, str], int] = {}

    start_ts = time.time()
    next_heartbeat = start_ts + SIG_HEARTBEAT_SEC

    while True:
        loop_start = time.time()
        total_signals_this_loop = 0

        for (symbol, tf), strat_list in universe.items():
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

            applicable_strats: List[Dict[str, Any]] = strat_list or []

            if applicable_strats:
                strat_names_str = ", ".join(s.get("name", f"sub-{s.get('sub_uid')}") for s in applicable_strats)
                msg = (
                    f"📡 *Signal Engine v2* — {symbol} / {tf_label}\n"
                    f"Side: *{side}*\n"
                    f"Strategies: `{strat_names_str}`\n"
                    f"Last close: `{last_c}` | MA({len([c['close'] for c in candles[-MA_LOOKBACK:]])}): `{ma}`\n"
                    f"Reason: `{reason}`\n"
                    f"(No orders placed here; executors handle trades.)"
                )
            else:
                msg = (
                    f"📡 *Signal Engine v2* — {symbol} / {tf_label}\n"
                    f"Side: *{side}*\n"
                    f"Last close: `{last_c}` | MA({len([c['close'] for c in candles[-MA_LOOKBACK:]])}): `{ma}`\n"
                    f"Reason: `{reason}`\n"
                    f"(No orders placed here; executors handle trades, generic universe.)"
                )

            tg_info(msg)

            # AI logging hook
            regime_tags = [reason]
            base_extra = {
                "engine": "signal_engine_v2",
                "raw_debug": debug,
                "tf_raw": tf,
            }

            if applicable_strats:
                for s in applicable_strats:
                    sub_uid = str(s.get("sub_uid"))
                    strat_name = s.get("name", f"sub-{sub_uid}")
                    automation_mode = s.get("automation_mode")
                    try:
                        sub_label = stratreg.get_sub_label(sub_uid) if _HAS_STRATEGY_REGISTRY else None
                    except Exception:
                        sub_label = None

                    extra = dict(base_extra)
                    extra.update(
                        {
                            "strategy_name": strat_name,
                            "strategy_automation_mode": automation_mode,
                            "sub_uid": sub_uid,
                            "sub_label": sub_label,
                            "strategy_raw": s.get("raw") or s,
                        }
                    )

                    signal_id = log_signal_from_engine(
                        symbol=symbol,
                        timeframe=tf_label,
                        side=side,
                        source="signal_engine_v2",
                        confidence=None,
                        stop_hint=None,
                        owner="AUTO_STRATEGY",
                        sub_uid=sub_uid,
                        strategy_role=strat_name,
                        regime_tags=regime_tags,
                        extra=extra,
                    )
                    print(
                        f"[SIGNAL] {symbol} {tf_label} {side} | "
                        f"strategy={strat_name} sub_uid={sub_uid} | signal_id={signal_id} | reason={reason}"
                    )

                    # JSONL export per strategy for auto_executor
                    append_signal_jsonl(
                        symbol=symbol,
                        side_text=side,
                        tf_label=tf_label,
                        bar_ts=bar_ts,
                        reason=reason,
                        debug=debug,
                        sub_uid=sub_uid,
                        strategy_name=strat_name,
                    )
            else:
                # Fallback: generic signal, no specific strategy
                extra = dict(base_extra)
                extra.update({"strategy_name": None, "strategy_automation_mode": None})
                signal_id = log_signal_from_engine(
                    symbol=symbol,
                    timeframe=tf_label,
                    side=side,
                    source="signal_engine_v2",
                    confidence=None,
                    stop_hint=None,
                    owner="AUTO_STRATEGY",
                    sub_uid=None,
                    strategy_role="GENERIC_SIGNAL_ENGINE",
                    regime_tags=regime_tags,
                    extra=extra,
                )
                print(
                    f"[SIGNAL] {symbol} {tf_label} {side} | "
                    f"strategy=GENERIC | signal_id={signal_id} | reason={reason}"
                )

                # JSONL export generic
                append_signal_jsonl(
                    symbol=symbol,
                    side_text=side,
                    tf_label=tf_label,
                    bar_ts=bar_ts,
                    reason=reason,
                    debug=debug,
                    sub_uid=None,
                    strategy_name=None,
                )

        # Heartbeat
        now = time.time()
        if now >= next_heartbeat:
            uptime_min = int((now - start_ts) / 60)
            hb = (
                f"🩺 Signal Engine heartbeat (v2)\n"
                f"- Uptime: {uptime_min} min\n"
                f"- Symbols: {', '.join(all_symbols)}\n"
                f"- TFs: {', '.join(tf_display(t) for t in all_tfs)}\n"
                f"- Last loop signals: {total_signals_this_loop}\n"
                f"- Using strategies: {SIG_USE_STRATEGIES and _HAS_STRATEGY_REGISTRY}\n"
                f"- AI logging: {'on' if _HAS_AI_HOOKS else 'OFF (stub)'}"
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
