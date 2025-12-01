#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — EMA Auto Trader (flashback03) v1.5

Account binding:
    - Trades ONLY on the Bybit subaccount whose keys are in:
        BYBIT_FLASHBACK03_API_KEY
        BYBIT_FLASHBACK03_API_SECRET

Features:
- 5m EMA(8/21) crossover with 1h EMA50 trend filter
- 1h ADX(14) > 20
- 5m volume Z-score > 0.5
- Liquidity + pump/dump filters
- Spread + depth guard
- 5% of equity per trade (RISK_ALLOC_PCT = 0.05)
- ALWAYS attempts cross margin + MAX leverage per symbol:
    • Reads leverageFilter.maxLeverage from /v5/market/instruments-info
    • Calls /v5/position/switch-isolated with tradeMode=0 (cross)
    • Calls /v5/position/set-leverage with that max value (buy/sell)
- REAL 7 TP limit orders (reduce-only) at 2R, 4R, 6R, 8R, 10R, 12R, 14R
- REAL hard SL (stop-market) at 2R from entry
- Software trailing SL:
    TP1 hit -> SL = entry
    TP2 hit -> SL = TP1
    ...
    When price crosses trailing SL -> cancel-all + market close
- Telegram:
    - Startup "ONLINE" notification
    - Entry notification
    - TP hit notifications
    - Final trade summary with R, PnL, equity change, rating 1–5
- AI logging:
    - Entries/exits to state/ema_auto_trader_trades_fb03.jsonl
    - Per-symbol performance in state/ema_symbol_profiles_fb03.json
- Trailing state in state/ema_trailing_state_fb03.json
- Equity-based max concurrent trades:
    < 50        -> 2 trades
    50–100      -> 3 trades
    100–250     -> 4 trades
    250–500     -> 5 trades
    500–1000    -> 6 trades
    >= 1000     -> 7 trades (cap)
"""

from __future__ import annotations

import os
import time
import hmac
import hashlib
import json
import threading
import math
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import requests
from dotenv import load_dotenv  # load .env

# Optional WebSocket client
try:
    import websocket  # type: ignore
except ImportError:
    websocket = None  # type: ignore

# -------------------- PATHS & ENV LOADING --------------------

# Project root: .../Flashback
ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

ENV_PATH = ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(ENV_PATH)
    print(f"[EMA fb03] Loaded .env from {ENV_PATH}")
else:
    print(f"[EMA fb03] WARNING: .env not found at {ENV_PATH}; using OS env only.")

# -------------------- CONFIG --------------------

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")
WS_PUBLIC_URL = os.getenv("BYBIT_WS_PUBLIC_LINEAR", "wss://stream.bybit.com/v5/public/linear")

# allow adjusting recv_window via .env, default 20000 ms
BYBIT_RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "20000")

# *** BOUND TO FLASHBACK03 SUBACCOUNT ***
API_KEY = os.getenv("BYBIT_FLASHBACK03_API_KEY", "")
API_SECRET = os.getenv("BYBIT_FLASHBACK03_API_SECRET", "")

if not API_KEY or not API_SECRET:
    print("!! WARNING: BYBIT_FLASHBACK03_API_KEY / _SECRET missing in env. This bot will NOT trade until set.")

ACCOUNT_TYPE = "UNIFIED"
CATEGORY = "linear"
SUB_LABEL = "flashback03"

# Timeframes & EMA settings
LTF_INTERVAL = "5"    # 5m
HTF_INTERVAL = "60"   # 1h
EMA_FAST = 8
EMA_SLOW = 21
EMA_HTF = 50

# ADX & volume z-score
ADX_PERIOD = 14
VOL_Z_LOOKBACK = 50
ADX_MIN = Decimal("20")
VOL_Z_MIN = Decimal("0.5")

# Risk / sizing
RISK_ALLOC_PCT = Decimal("0.05")   # 5% of equity as notional per trade

# Liquidity & pump/dump thresholds
MIN_24H_TURNOVER_USDT = Decimal("5000000")  # 5M
MIN_24H_VOLUME = Decimal("500000")
MAX_ABS_24H_CHANGE_PCT = Decimal("40")

# Spread & depth guard
MAX_SPREAD_PCT = Decimal("0.15")   # max allowed spread
DEPTH_MULTIPLIER = Decimal("3")    # depth must be >= 3x notional

# ATR / R / 7TP ladder
ATR_PERIOD = 14
ATR_MULT_R = Decimal("1.0")               # base R unit = ATR * this
NUM_TPS = 7
TP_R_MULTS = [Decimal("2"), Decimal("4"), Decimal("6"),
              Decimal("8"), Decimal("10"), Decimal("12"), Decimal("14")]
STOP_INITIAL_R = Decimal("2")             # SL distance at start: 2R

# WS candidate logic
WS_ENABLED = True
PRICE_MOVE_THRESHOLD_PCT = Decimal("0.20")  # 0.20% vs anchor
CANDIDATE_TIMEOUT_SEC = 300                 # 5 minutes
MAX_CANDIDATES_PER_LOOP = 40
TOP_LIQUID_FALLBACK = 30

# Loop timing
LOOP_SLEEP_SEC = 30

# Per-bot filenames
AI_LOG_PATH = STATE_DIR / "ema_auto_trader_trades_fb03.jsonl"
TRAIL_STATE_PATH = STATE_DIR / "ema_trailing_state_fb03.json"
PROFILES_PATH = STATE_DIR / "ema_symbol_profiles_fb03.json"

# Telegram (can reuse same values as main EMA bot if you want)
TG_TOKEN = os.getenv("TG_TOKEN_EMA_AUTO", "")
TG_CHAT = os.getenv("TG_CHAT_EMA_AUTO", "")

# -------------------- WS shared state --------------------

latest_tickers_lock = threading.Lock()
latest_tickers: Dict[str, Dict[str, Any]] = {}
price_anchors: Dict[str, Decimal] = {}
candidate_symbols: Dict[str, float] = {}  # symbol -> last_mark_ts

# -------------------- Bybit signing --------------------

def _sign(timestamp: str, recv_window: str, query_string: str, body: str) -> str:
    payload = timestamp + API_KEY + recv_window + query_string + body
    return hmac.new(
        API_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def bybit_request(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Bybit API key/secret is not configured for flashback03.")

    url = BYBIT_BASE + path
    params = params or {}
    body = body or {}

    items = sorted((k, str(v)) for k, v in params.items())
    query_string = "&".join(f"{k}={v}" for k, v in items)

    body_str = json.dumps(body) if body else ""
    ts = str(int(time.time() * 1000))
    recv_window = BYBIT_RECV_WINDOW

    sign = _sign(ts, recv_window, query_string, body_str)

    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json",
    }

    if method.upper() == "GET":
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    else:
        resp = requests.post(url, params=params, data=body_str, headers=headers, timeout=timeout)

    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") not in (0, "0"):
        # Surface timestamp problems clearly
        raise RuntimeError(f"Bybit error {data.get('retCode')}: {data.get('retMsg')}")
    return data


# -------------------- Telegram helpers --------------------

def tg_send(text: str) -> None:
    prefix = f"[{SUB_LABEL}] "
    text = prefix + text
    if not TG_TOKEN or not TG_CHAT:
        print(f"[TG disabled] {text}")
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        resp = requests.post(url, json={"chat_id": TG_CHAT, "text": text}, timeout=5)
        if resp.status_code != 200:
            print(f"[TG error] status={resp.status_code}, body={resp.text}")
    except Exception as e:
        print(f"[TG exception] {e}")


# -------------------- Indicators --------------------

def ema(series: List[Decimal], length: int) -> List[Decimal]:
    if not series or length <= 0:
        return []
    k = Decimal("2") / (Decimal(length) + Decimal("1"))
    ema_vals: List[Decimal] = []
    ema_prev: Optional[Decimal] = None
    for price in series:
        if ema_prev is None:
            ema_prev = price
        else:
            ema_prev = price * k + ema_prev * (Decimal("1") - k)
        ema_vals.append(ema_prev)
    return ema_vals


def atr(candles: List[Dict[str, Any]], period: int) -> Decimal:
    if len(candles) < period + 1:
        return Decimal("0")
    trs: List[Decimal] = []
    for i in range(1, len(candles)):
        curr = candles[i]
        prev = candles[i - 1]
        high = curr["high"]
        low = curr["low"]
        prev_close = prev["close"]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)
    trs = trs[-period:]
    if not trs:
        return Decimal("0")
    return sum(trs) / Decimal(len(trs))


def adx(candles: List[Dict[str, Any]], period: int) -> Decimal:
    if len(candles) < period + 2:
        return Decimal("0")

    plus_dm: List[Decimal] = []
    minus_dm: List[Decimal] = []
    trs: List[Decimal] = []

    for i in range(1, len(candles)):
        curr = candles[i]
        prev = candles[i - 1]
        up_move = curr["high"] - prev["high"]
        down_move = prev["low"] - curr["low"]

        if up_move > down_move and up_move > 0:
            plus_dm.append(up_move)
        else:
            plus_dm.append(Decimal("0"))

        if down_move > up_move and down_move > 0:
            minus_dm.append(down_move)
        else:
            minus_dm.append(Decimal("0"))

        tr = max(
            curr["high"] - curr["low"],
            abs(curr["high"] - prev["close"]),
            abs(curr["low"] - prev["close"]),
        )
        trs.append(tr)

    if len(trs) < period:
        return Decimal("0")

    def wilder_smooth(values: List[Decimal], n: int) -> List[Decimal]:
        if len(values) < n:
            return []
        smoothed: List[Decimal] = []
        first = sum(values[:n])
        smoothed.append(first)
        prev = first
        for v in values[n:]:
            prev = prev - (prev / Decimal(n)) + v
            smoothed.append(prev)
        return smoothed

    tr_smooth = wilder_smooth(trs, period)
    plus_smooth = wilder_smooth(plus_dm, period)
    minus_smooth = wilder_smooth(minus_dm, period)

    if not tr_smooth or not plus_smooth or not minus_smooth:
        return Decimal("0")

    di_plus: List[Decimal] = []
    di_minus: List[Decimal] = []

    for trv, pdm, mdm in zip(tr_smooth, plus_smooth, minus_smooth):
        if trv == 0:
            di_plus.append(Decimal("0"))
            di_minus.append(Decimal("0"))
        else:
            di_plus.append((pdm / trv) * Decimal("100"))
            di_minus.append((mdm / trv) * Decimal("100"))

    dx_vals: List[Decimal] = []
    for p, m in zip(di_plus, di_minus):
        denom = p + m
        if denom == 0:
            dx_vals.append(Decimal("0"))
        else:
            dx_vals.append((abs(p - m) / denom) * Decimal("100"))

    if len(dx_vals) < period:
        return Decimal("0")

    def wilder_avg(vals: List[Decimal], n: int) -> List[Decimal]:
        if len(vals) < n:
            return []
        first = sum(vals[:n]) / Decimal(n)
        out: List[Decimal] = [first]
        prev = first
        for v in vals[n:]:
            prev = ((prev * (Decimal(n) - Decimal("1"))) + v) / Decimal(n)
            out.append(prev)
        return out

    dx_smooth = wilder_avg(dx_vals, period)
    if not dx_smooth:
        return Decimal("0")

    return dx_smooth[-1]


def volume_z_score(candles: List[Dict[str, Any]], lookback: int = 50) -> Decimal:
    vols = [c["volume"] for c in candles]
    if len(vols) < lookback + 1:
        return Decimal("0")
    recent = vols[-lookback:]
    v_now = vols[-1]
    mean = sum(recent) / Decimal(len(recent))
    if mean <= 0:
        return Decimal("0")
    var = sum((v - mean) ** 2 for v in recent) / Decimal(len(recent))
    std = Decimal(str(math.sqrt(float(var))))
    if std == 0:
        return Decimal("0")
    return (v_now - mean) / std


# -------------------- Market data helpers --------------------

def get_linear_instruments() -> List[Dict[str, Any]]:
    data = bybit_request("GET", "/v5/market/instruments-info", {"category": CATEGORY, "limit": 1000})
    return data.get("result", {}).get("list", []) or []


def get_tickers_rest() -> Dict[str, Dict[str, Any]]:
    data = bybit_request("GET", "/v5/market/tickers", {"category": CATEGORY})
    rows = data.get("result", {}).get("list", []) or []
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        sym = r.get("symbol")
        if sym:
            out[sym] = r
    return out


def fetch_klines(symbol: str, interval: str, limit: int = 200) -> List[Dict[str, Any]]:
    data = bybit_request("GET", "/v5/market/kline", {
        "category": CATEGORY,
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit),
    })
    rows = data.get("result", {}).get("list", []) or []
    rows = list(reversed(rows))
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append({
            "start": int(row[0]),
            "open": Decimal(row[1]),
            "high": Decimal(row[2]),
            "low": Decimal(row[3]),
            "close": Decimal(row[4]),
            "volume": Decimal(row[5]),
            "turnover": Decimal(row[6]),
        })
    return out


def get_orderbook(symbol: str, limit: int = 50) -> Dict[str, Any]:
    data = bybit_request("GET", "/v5/market/orderbook", {
        "category": CATEGORY,
        "symbol": symbol,
        "limit": str(limit),
    })
    return data.get("result", {}) or {}


# -------------------- Account / positions / orders --------------------

def get_equity_usdt() -> Decimal:
    data = bybit_request("GET", "/v5/account/wallet-balance", {
        "accountType": ACCOUNT_TYPE,
        "coin": "USDT",
    })
    lst = data.get("result", {}).get("list", []) or []
    if not lst:
        return Decimal("0")
    acct = lst[0]
    eq_str = acct.get("totalEquity") or acct.get("totalWalletBalance") or "0"
    try:
        return Decimal(str(eq_str))
    except Exception:
        return Decimal("0")


def get_open_positions() -> Dict[str, Dict[str, Any]]:
    data = bybit_request("GET", "/v5/position/list", {
        "category": CATEGORY,
        "settleCoin": "USDT",
    })
    rows = data.get("result", {}).get("list", []) or []
    out: Dict[str, Dict[str, Any]] = {}
    for p in rows:
        sym = p.get("symbol")
        if not sym:
            continue
        size = Decimal(p.get("size", "0") or "0")
        if size == 0:
            continue
        out[sym] = p
    return out


def place_market_order(
    sym: str,
    direction: str,
    notional_usd: Decimal,
    price: Decimal,
) -> Tuple[str, Decimal]:
    side = "Buy" if direction == "LONG" else "Sell"
    qty = notional_usd / price
    qty = qty.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if qty <= 0:
        raise RuntimeError("Computed qty <= 0 (equity too small / price too high)")

    body = {
        "category": CATEGORY,
        "symbol": sym,
        "side": side,
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "IOC",
        "reduceOnly": False,
    }
    _ = bybit_request("POST", "/v5/order/create", body=body)
    return side, qty


def close_position_market(sym: str, direction: str, size: Decimal) -> None:
    if size <= 0:
        return
    side = "Sell" if direction == "LONG" else "Buy"
    qty = size.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if qty <= 0:
        return
    body = {
        "category": CATEGORY,
        "symbol": sym,
        "side": side,
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "IOC",
        "reduceOnly": True,
    }
    try:
        bybit_request("POST", "/v5/order/create", body=body)
    except Exception as e:
        print(f"[{sym}] close_position_market error: {e}")


def cancel_all_orders(sym: str) -> None:
    body = {
        "category": CATEGORY,
        "symbol": sym,
    }
    try:
        bybit_request("POST", "/v5/order/cancel-all", body=body)
    except Exception as e:
        print(f"[{sym}] cancel-all error: {e}")


def get_closed_pnl_latest(sym: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    try:
        data = bybit_request("GET", "/v5/position/closed-pnl", {
            "category": CATEGORY,
            "symbol": sym,
            "limit": "1",
        })
        rows = data.get("result", {}).get("list", []) or []
        if not rows:
            return None, None
        row = rows[0]
        pnl = Decimal(str(row.get("closedPnl", "0")))
        exit_px_raw = row.get("avgExitPrice")
        exit_px = Decimal(str(exit_px_raw)) if exit_px_raw not in (None, "", "0") else None
        return pnl, exit_px
    except Exception:
        return None, None


def place_tp_sl_orders(
    sym: str,
    direction: str,
    qty: Decimal,
    entry_price: Decimal,
    R: Decimal,
    tp_levels: List[Decimal],
    sl_level: Decimal,
) -> None:
    q_unit = (qty / Decimal(NUM_TPS)).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if q_unit <= 0:
        return

    chunks: List[Decimal] = [q_unit] * NUM_TPS
    total_chunks = q_unit * Decimal(NUM_TPS)
    remainder = qty - total_chunks
    if remainder > 0:
        chunks[-1] = (chunks[-1] + remainder).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

    tp_side = "Sell" if direction == "LONG" else "Buy"

    for i, (tp_price, q_chunk) in enumerate(zip(tp_levels, chunks), start=1):
        if q_chunk <= 0:
            continue
        body = {
            "category": CATEGORY,
            "symbol": sym,
            "side": tp_side,
            "orderType": "Limit",
            "qty": str(q_chunk),
            "price": str(tp_price),
            "timeInForce": "GTC",
            "reduceOnly": True,
        }
        try:
            bybit_request("POST", "/v5/order/create", body=body)
        except Exception as e:
            print(f"[{sym}] TP{i} order error: {e}")

    sl_side = "Sell" if direction == "LONG" else "Buy"
    body_sl = {
        "category": CATEGORY,
        "symbol": sym,
        "side": sl_side,
        "orderType": "Market",
        "timeInForce": "GTC",
        "reduceOnly": True,
        "triggerDirection": 1 if direction == "LONG" else 2,
        "triggerPrice": str(sl_level),
        "qty": str(qty),
        "positionIdx": 0,
    }
    try:
        bybit_request("POST", "/v5/order/create", body=body_sl)
    except Exception as e:
        print(f"[{sym}] SL order error: {e}")


# -------------------- AI logging + profiles + trailing state --------------------

def ai_log_event(event: Dict[str, Any]) -> None:
    event = dict(event)
    event["ts"] = int(time.time() * 1000)
    AI_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with AI_LOG_PATH.open("ab") as f:
        line = json.dumps(event, default=str).encode("utf-8")
        f.write(line + b"\n")


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default


def save_json(path: Path, data: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, default=str)
        tmp.replace(path)
    except Exception as e:
        print(f"[STATE] save_json error {path}: {e}")


def load_trailing_state() -> Dict[str, Dict[str, Any]]:
    return load_json(TRAIL_STATE_PATH, {})


def save_trailing_state(state: Dict[str, Dict[str, Any]]) -> None:
    save_json(TRAIL_STATE_PATH, state)


def load_profiles() -> Dict[str, Dict[str, Any]]:
    return load_json(PROFILES_PATH, {})


def save_profiles(profiles: Dict[str, Dict[str, Any]]) -> None:
    save_json(PROFILES_PATH, profiles)


def update_symbol_profile(
    profiles: Dict[str, Dict[str, Any]],
    symbol: str,
    realized_R: Decimal,
) -> None:
    prof = profiles.get(symbol) or {
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "avg_R": 0.0,
    }

    trades = int(prof.get("trades", 0)) + 1
    wins = int(prof.get("wins", 0))
    losses = int(prof.get("losses", 0))
    prev_avg_R = Decimal(str(prof.get("avg_R", 0.0)))

    if realized_R > 0:
        wins += 1
    elif realized_R < 0:
        losses += 1

    new_avg_R = (prev_avg_R * Decimal(str(trades - 1)) + realized_R) / Decimal(str(trades))

    prof["trades"] = trades
    prof["wins"] = wins
    prof["losses"] = losses
    prof["avg_R"] = float(new_avg_R)

    profiles[symbol] = prof


def symbol_is_banned(profiles: Dict[str, Dict[str, Any]], symbol: str) -> bool:
    prof = profiles.get(symbol)
    if not prof:
        return False
    trades = int(prof.get("trades", 0))
    avg_R = Decimal(str(prof.get("avg_R", 0.0)))
    if trades >= 10 and avg_R <= Decimal("-0.2"):
        return True
    return False


def rate_trade_1_5(realized_R: Decimal) -> int:
    if realized_R >= Decimal("3"):
        base = 5
    elif realized_R >= Decimal("2"):
        base = 5
    elif realized_R >= Decimal("1"):
        base = 4
    elif realized_R >= Decimal("0.3"):
        base = 3
    elif realized_R > Decimal("-0.5"):
        base = 2
    elif realized_R > Decimal("-1.5"):
        base = 2
    else:
        base = 1
    return max(1, min(5, base))


# -------------------- WS ticker stream --------------------

class BybitTickerStream(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        if websocket is None:
            print("[WS] websocket-client not installed, WS disabled.")
            return
        while not self._stop.is_set():
            try:
                self._run_once()
            except Exception as e:
                print(f"[WS] error: {e}")
                time.sleep(5)

    def _run_once(self) -> None:
        def on_open(ws: "websocket.WebSocketApp") -> None:  # type: ignore[name-defined]
            msg = {
                "op": "subscribe",
                "args": ["tickers.linear"],
            }
            ws.send(json.dumps(msg))
            print("[WS] Connected and subscribed to tickers.linear")

        def on_message(ws: "websocket.WebSocketApp", message: str) -> None:  # type: ignore[name-defined]
            try:
                data = json.loads(message)
            except Exception:
                return
            if data.get("topic", "").startswith("tickers"):
                items = data.get("data") or data.get("d") or []
                if isinstance(items, dict):
                    items = [items]
                now_ts = time.time()

                for t in items:
                    sym = t.get("symbol")
                    if not sym:
                        continue
                    last_price_raw = t.get("lastPrice") or t.get("lastPriceEp") or t.get("last_price")
                    try:
                        last_price = Decimal(str(last_price_raw))
                    except Exception:
                        continue

                    with latest_tickers_lock:
                        latest_tickers[sym] = t
                        anchor = price_anchors.get(sym)
                        if anchor is None:
                            price_anchors[sym] = last_price
                            continue
                        if anchor > 0:
                            pct_move = (last_price - anchor) / anchor * Decimal("100")
                        else:
                            pct_move = Decimal("0")
                        if abs(pct_move) >= PRICE_MOVE_THRESHOLD_PCT:
                            candidate_symbols[sym] = now_ts
                            price_anchors[sym] = last_price

        def on_error(ws: "websocket.WebSocketApp", error: Any) -> None:  # type: ignore[name-defined]
            print(f"[WS] error callback: {error}")

        def on_close(ws: "websocket.WebSocketApp", status_code: Any, msg: Any) -> None:  # type: ignore[name-defined]
            print(f"[WS] closed: {status_code} {msg}")

        ws_app = websocket.WebSocketApp(  # type: ignore[call-arg]
            WS_PUBLIC_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws_app.run_forever(ping_interval=20, ping_timeout=10)


def get_latest_tickers() -> Dict[str, Dict[str, Any]]:
    with latest_tickers_lock:
        if latest_tickers:
            return dict(latest_tickers)
    try:
        return get_tickers_rest()
    except Exception as e:
        print(f"[TICKERS] REST fallback failed: {e}")
        return {}


def select_candidate_symbols(
    inst_by_symbol: Dict[str, Dict[str, Any]],
    tickers: Dict[str, Dict[str, Any]],
) -> List[str]:
    now_ts = time.time()
    with latest_tickers_lock:
        stale = [s for s, ts in candidate_symbols.items() if now_ts - ts > CANDIDATE_TIMEOUT_SEC]
        for s in stale:
            candidate_symbols.pop(s, None)
        ws_candidates = list(candidate_symbols.keys())

    universe = sorted(set(inst_by_symbol.keys()) & set(tickers.keys()))

    def turnover(sym: str) -> Decimal:
        try:
            return Decimal(tickers[sym].get("turnover24h", "0") or "0")
        except Exception:
            return Decimal("0")

    universe_sorted = sorted(universe, key=turnover, reverse=True)
    top_liquid = universe_sorted[:TOP_LIQUID_FALLBACK]

    merged = list(dict.fromkeys(ws_candidates + top_liquid))
    return merged[:MAX_CANDIDATES_PER_LOOP]


# -------------------- Filters & signals --------------------

def filter_symbol(sym: str, inst: Dict[str, Any], ticker: Dict[str, Any]) -> bool:
    try:
        turnover_24h = Decimal(ticker.get("turnover24h", "0") or "0")
        volume_24h = Decimal(ticker.get("volume24h", "0") or "0")
        change24h = Decimal(ticker.get("price24hPcnt", "0") or "0") * Decimal("100")
    except Exception:
        return False

    if turnover_24h < MIN_24H_TURNOVER_USDT:
        return False
    if volume_24h < MIN_24H_VOLUME:
        return False
    if abs(change24h) > MAX_ABS_24H_CHANGE_PCT:
        return False
    if inst.get("status") != "Trading":
        return False
    return True


def spread_and_depth_ok(sym: str, direction: str, notional: Decimal) -> bool:
    try:
        ob = get_orderbook(sym, limit=50)
    except Exception as e:
        print(f"[{sym}] orderbook error: {e}")
        return False

    bids = ob.get("b", []) or ob.get("bid", []) or []
    asks = ob.get("a", []) or ob.get("ask", []) or []

    def parse_side(levels: List[List[str]]) -> List[Tuple[Decimal, Decimal]]:
        out: List[Tuple[Decimal, Decimal]] = []
        for lvl in levels:
            if len(lvl) < 2:
                continue
            try:
                price = Decimal(str(lvl[0]))
                qty = Decimal(str(lvl[1]))
                out.append((price, qty))
            except Exception:
                continue
        return out

    bids_parsed = parse_side(bids)
    asks_parsed = parse_side(asks)
    if not bids_parsed or not asks_parsed:
        return False

    best_bid, _ = bids_parsed[0]
    best_ask, _ = asks_parsed[0]
    mid = (best_bid + best_ask) / Decimal("2")
    if mid <= 0:
        return False
    spread_pct = (best_ask - best_bid) / mid * Decimal("100")
    if spread_pct > MAX_SPREAD_PCT:
        print(f"[{sym}] spread too high: {spread_pct:.4f}%")
        return False

    target_side = asks_parsed if direction == "LONG" else bids_parsed
    depth_usdt = Decimal("0")
    for price, qty in target_side:
        depth_usdt += price * qty
        if depth_usdt >= notional * DEPTH_MULTIPLIER:
            break

    if depth_usdt < notional * DEPTH_MULTIPLIER:
        print(f"[{sym}] depth too low: {depth_usdt} < {notional * DEPTH_MULTIPLIER}")
        return False
    return True


def get_signal(sym: str) -> Optional[Dict[str, Any]]:
    try:
        ltf = fetch_klines(sym, LTF_INTERVAL, limit=200)
        htf = fetch_klines(sym, HTF_INTERVAL, limit=200)
    except Exception as e:
        print(f"[{sym}] kline error: {e}")
        return None

    if len(ltf) < max(EMA_FAST, EMA_SLOW) + 2:
        return None
    if len(htf) < ADX_PERIOD + EMA_HTF + 2:
        return None

    closes_ltf = [c["close"] for c in ltf]
    closes_htf = [c["close"] for c in htf]

    ema_fast = ema(closes_ltf, EMA_FAST)
    ema_slow = ema(closes_ltf, EMA_SLOW)
    ema50_htf = ema(closes_htf, EMA_HTF)

    f_prev, f_now = ema_fast[-2], ema_fast[-1]
    s_prev, s_now = ema_slow[-2], ema_slow[-1]
    price_now = closes_ltf[-1]

    ema50_prev, ema50_now = ema50_htf[-2], ema50_htf[-1]

    bullish_cross = f_prev < s_prev and f_now > s_now
    bearish_cross = f_prev > s_prev and f_now < s_now

    bullish_trend = price_now > ema50_now and ema50_now > ema50_prev
    bearish_trend = price_now < ema50_now and ema50_now < ema50_prev

    direction: Optional[str] = None
    if bullish_cross and bullish_trend:
        direction = "LONG"
    elif bearish_cross and bearish_trend:
        direction = "SHORT"

    if direction is None:
        return None

    adx_val = adx(htf, ADX_PERIOD)
    vol_z = volume_z_score(ltf, VOL_Z_LOOKBACK)
    if adx_val < ADX_MIN or vol_z < VOL_Z_MIN:
        return None

    atr_val = atr(ltf, ATR_PERIOD)
    if atr_val <= 0:
        return None
    R = atr_val * ATR_MULT_R

    return {
        "symbol": sym,
        "direction": direction,
        "price": price_now,
        "R": R,
        "adx": adx_val,
        "vol_z": vol_z,
    }


# -------------------- TP / SL helpers --------------------

def build_tp_levels(entry: Decimal, direction: str, R: Decimal) -> List[Decimal]:
    tps: List[Decimal] = []
    is_long = (direction == "LONG")
    for mult in TP_R_MULTS:
        if is_long:
            tps.append(entry + mult * R)
        else:
            tps.append(entry - mult * R)
    return tps


def initial_sl_level(entry: Decimal, direction: str, R: Decimal) -> Decimal:
    if direction == "LONG":
        return entry - STOP_INITIAL_R * R
    else:
        return entry + STOP_INITIAL_R * R


def update_trailing_for_open_positions(
    trail_state: Dict[str, Dict[str, Any]],
    open_positions: Dict[str, Dict[str, Any]],
    tickers: Dict[str, Dict[str, Any]],
    profiles: Dict[str, Dict[str, Any]],
) -> None:
    to_delete: List[str] = []

    for sym, state in list(trail_state.items()):
        direction = state["direction"]
        entry_price = Decimal(str(state["entry_price"]))
        R = Decimal(str(state["R"]))
        tps = [Decimal(str(x)) for x in state["tp_levels"]]
        sl_level = Decimal(str(state["sl_level"]))
        qty_initial = Decimal(str(state["qty_initial"]))
        last_tp_index = int(state.get("last_tp_index", 0))
        risk_usd = Decimal(str(state["risk_usd"]))
        eq_entry = Decimal(str(state["equity_at_entry"]))

        pos = open_positions.get(sym)

        if pos is None:
            pnl, exit_px = get_closed_pnl_latest(sym)
            equity_now = get_equity_usdt()
            if pnl is not None and risk_usd > 0:
                realized_R = pnl / risk_usd
            else:
                realized_R = Decimal("0")
            rating = rate_trade_1_5(realized_R)

            msg = (
                f"🔴 TRADE CLOSED ({sym} {direction})\n"
                f"Entry: {entry_price}\n"
                f"Exit: {exit_px if exit_px is not None else 'n/a'}\n"
                f"PnL: {pnl} USDT\n"
                f"R: {realized_R:.2f}\n"
                f"Equity: {eq_entry} → {equity_now}\n"
                f"Rating: {rating}/5"
            )
            tg_send(msg)

            ai_log_event({
                "type": "exit",
                "symbol": sym,
                "direction": direction,
                "entry_price": str(entry_price),
                "exit_price": str(exit_px) if exit_px is not None else None,
                "realized_pnl": str(pnl) if pnl is not None else None,
                "realized_R": str(realized_R),
                "equity_at_entry": str(eq_entry),
                "equity_after": str(equity_now),
                "rating_1_5": rating,
            })

            update_symbol_profile(profiles, sym, realized_R)
            to_delete.append(sym)
            continue

        try:
            size_now = Decimal(str(pos.get("size", "0") or "0"))
        except Exception:
            size_now = Decimal("0")

        if size_now <= 0:
            continue

        q_unit = qty_initial / Decimal(str(NUM_TPS))
        if q_unit <= 0:
            k_theoretical = 0
        else:
            k_theoretical = int(((qty_initial - size_now) / q_unit + Decimal("0.01")))
        if k_theoretical < 0:
            k_theoretical = 0
        if k_theoretical > NUM_TPS:
            k_theoretical = NUM_TPS

        if k_theoretical > last_tp_index:
            for tp_idx in range(last_tp_index + 1, k_theoretical + 1):
                if tp_idx == 1:
                    new_sl = entry_price
                else:
                    prev_tp = tps[tp_idx - 2]
                    new_sl = prev_tp
                sl_level = new_sl
                msg = (
                    f"🎯 TP{tp_idx} HIT ({sym} {direction})\n"
                    f"SL (software) moved to {sl_level}"
                )
                tg_send(msg)

            state["last_tp_index"] = k_theoretical
            state["sl_level"] = str(sl_level)

        t = tickers.get(sym)
        if t:
            last_price_raw = t.get("lastPrice") or t.get("lastPriceEp") or t.get("last_price")
            try:
                last_price = Decimal(str(last_price_raw))
            except Exception:
                last_price = None
            if last_price is not None:
                if direction == "LONG" and last_price <= sl_level:
                    tg_send(
                        f"⛔ Trailing SL triggered ({sym} LONG) @ {last_price}, "
                        f"SL={sl_level} — canceling orders & closing."
                    )
                    cancel_all_orders(sym)
                    close_position_market(sym, direction, size_now)
                elif direction == "SHORT" and last_price >= sl_level:
                    tg_send(
                        f"⛔ Trailing SL triggered ({sym} SHORT) @ {last_price}, "
                        f"SL={sl_level} — canceling orders & closing."
                    )
                    cancel_all_orders(sym)
                    close_position_market(sym, direction, size_now)

        trail_state[sym] = state

    for sym in to_delete:
        trail_state.pop(sym, None)

    save_trailing_state(trail_state)
    save_profiles(profiles)


# -------------------- Leverage helpers (max leverage + cross) --------------------

def get_symbol_max_leverage(inst: Dict[str, Any]) -> str:
    """
    Extract maxLeverage from instruments-info leverageFilter.
    Fallback to '50' if not present, because Bybit.
    """
    lev_filter = inst.get("leverageFilter") or {}
    max_lev = lev_filter.get("maxLeverage") or lev_filter.get("maxLeverageE")
    if max_lev in (None, "", "0"):
        return "50"
    return str(max_lev)


def ensure_cross_max_leverage(sym: str, inst: Dict[str, Any]) -> None:
    """
    Try to:
      1) Switch margin mode to cross (tradeMode=0) for this symbol.
      2) Set buyLeverage & sellLeverage to that symbol's max leverage.
    Failures are logged but NOT fatal to the entry.
    """
    max_lev = get_symbol_max_leverage(inst)

    # 1) Switch to cross margin (tradeMode=0) if possible
    try:
        body_mode = {
            "category": CATEGORY,
            "symbol": sym,
            "tradeMode": 0,           # 0 = cross, 1 = isolated (per Bybit v5 docs)
            "buyLeverage": max_lev,
            "sellLeverage": max_lev,
        }
        bybit_request("POST", "/v5/position/switch-isolated", body=body_mode)
        print(f"[{sym}] switched to CROSS margin, leverage={max_lev}x.")
    except Exception as e:
        print(f"[{sym}] switch-isolated (cross) failed: {e}")

    # 2) Explicitly set leverage to max (in case margin mode already cross)
    try:
        body_lev = {
            "category": CATEGORY,
            "symbol": sym,
            "buyLeverage": max_lev,
            "sellLeverage": max_lev,
        }
        bybit_request("POST", "/v5/position/set-leverage", body=body_lev)
        print(f"[{sym}] set-leverage to {max_lev}x (buy/sell).")
    except Exception as e:
        print(f"[{sym}] set-leverage failed: {e}")


# -------------------- Equity-based max trades --------------------

def max_trades_for_equity(eq: Decimal) -> int:
    if eq < Decimal("50"):
        return 2
    elif eq < Decimal("100"):
        return 3
    elif eq < Decimal("250"):
        return 4
    elif eq < Decimal("500"):
        return 5
    elif eq < Decimal("1000"):
        return 6
    else:
        return 7


# -------------------- Main loop --------------------

def main_loop() -> None:
    print(f"=== EMA Auto Trader v1.5 ({SUB_LABEL}) ===")
    print(f"Root: {ROOT}")
    print(f"State: {STATE_DIR}")
    print(f"Bybit base: {BYBIT_BASE}")
    print(f"recv_window: {BYBIT_RECV_WINDOW} ms")

    # startup heartbeat to Telegram
    tg_send(
        "✅ EMA Auto Trader ONLINE\n"
        f"Sub: {SUB_LABEL}\n"
        f"Bybit: {BYBIT_BASE}\n"
        f"recv_window: {BYBIT_RECV_WINDOW} ms"
    )

    profiles = load_profiles()
    trail_state = load_trailing_state()

    ws_thread: Optional[BybitTickerStream] = None
    if WS_ENABLED and websocket is not None:
        ws_thread = BybitTickerStream()
        ws_thread.start()
        print("[MAIN] WebSocket ticker stream started.")
    else:
        if websocket is None:
            print("[MAIN] websocket-client not installed; WS disabled.")
        else:
            print("[MAIN] WS_ENABLED=False; WS disabled.")

    try:
        while True:
            try:
                equity = get_equity_usdt()
                if equity <= 0:
                    print("[MAIN] Equity is zero/negative, sleeping.")
                    time.sleep(LOOP_SLEEP_SEC)
                    continue

                max_open_trades = max_trades_for_equity(equity)

                open_positions = get_open_positions()
                open_count = len(open_positions)

                print(
                    f"[MAIN] [{SUB_LABEL}] Equity={equity} | max_open_trades={max_open_trades} | "
                    f"current_open={open_count}"
                )

                if open_count >= max_open_trades:
                    print("[MAIN] At max allowed trades for current equity, managing only.")
                    tickers = get_latest_tickers()
                    update_trailing_for_open_positions(trail_state, open_positions, tickers, profiles)
                    time.sleep(LOOP_SLEEP_SEC)
                    continue

                instruments = get_linear_instruments()
                inst_by_symbol = {i["symbol"]: i for i in instruments}

                tickers = get_latest_tickers()
                if not tickers:
                    print("[MAIN] No tickers available, sleeping.")
                    time.sleep(LOOP_SLEEP_SEC)
                    continue

                update_trailing_for_open_positions(trail_state, open_positions, tickers, profiles)

                candidates = select_candidate_symbols(inst_by_symbol, tickers)
                if not candidates:
                    print("[MAIN] No candidates, sleeping.")
                    time.sleep(LOOP_SLEEP_SEC)
                    continue

                print(
                    f"[MAIN] open_positions={len(open_positions)} | "
                    f"candidates={len(candidates)}"
                )

                for sym in candidates:
                    if sym in open_positions:
                        continue

                    inst = inst_by_symbol.get(sym)
                    tic = tickers.get(sym)
                    if not inst or not tic:
                        continue

                    if symbol_is_banned(profiles, sym):
                        print(f"[{sym}] banned by profile (avg_R too low).")
                        continue

                    if not filter_symbol(sym, inst, tic):
                        continue

                    notional = (equity * RISK_ALLOC_PCT).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                    if notional <= 0:
                        print(f"[{sym}] Notional <= 0 for current equity, skipping.")
                        continue

                    signal = get_signal(sym)
                    if not signal:
                        continue

                    direction = signal["direction"]
                    price = signal["price"]
                    R = signal["R"]
                    adx_val = signal["adx"]
                    vol_z = signal["vol_z"]

                    if not spread_and_depth_ok(sym, direction, notional):
                        continue

                    # *** NEW: enforce CROSS + MAX LEVERAGE for this symbol before entry ***
                    ensure_cross_max_leverage(sym, inst)

                    print(
                        f"[{sym}] Signal {direction} @ {price} | notional≈{notional} | "
                        f"R≈{R:.6f} | ADX={adx_val:.2f} | volZ={vol_z:.2f}"
                    )

                    try:
                        side, qty = place_market_order(sym, direction, notional, price)
                    except Exception as e:
                        msg = f"[{sym}] entry order error: {e}"
                        print(msg)
                        tg_send(f"⚠️ Entry failed for {sym}: {e}")
                        continue

                    tp_levels = build_tp_levels(price, direction, R)
                    sl_level = initial_sl_level(price, direction, R)
                    risk_per_unit = STOP_INITIAL_R * R
                    risk_usd = (risk_per_unit * qty).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

                    place_tp_sl_orders(sym, direction, qty, price, R, tp_levels, sl_level)

                    trail_state[sym] = {
                        "direction": direction,
                        "entry_price": str(price),
                        "R": str(R),
                        "tp_levels": [str(tp) for tp in tp_levels],
                        "sl_level": str(sl_level),
                        "qty_initial": str(qty),
                        "last_tp_index": 0,
                        "risk_usd": str(risk_usd),
                        "equity_at_entry": str(equity),
                    }
                    save_trailing_state(trail_state)

                    msg_lines = [
                        "🟢 EMA ENTRY",
                        f"Sub: {SUB_LABEL}",
                        f"Pair: {sym} ({direction})",
                        f"Entry: {price}",
                        f"Size: {qty} (notional≈{notional} USDT)",
                        f"R unit: {R:.6f}",
                        f"Initial SL: {sl_level} (hard stop at 2R)",
                        "TPs:",
                    ]
                    for i, tp in enumerate(tp_levels, start=1):
                        msg_lines.append(f"  • TP{i}: {tp}")
                    msg_lines.append(f"Equity: {equity}")
                    msg_lines.append(f"Regime: ADX(1h)={adx_val:.2f}, VolZ(5m)={vol_z:.2f}")
                    msg_lines.append("Reason: 5m EMA(8/21) cross + 1h EMA50 trend + ADX>20 + VolZ>0.5 + spread/depth OK")

                    tg_send("\n".join(msg_lines))

                    ai_log_event({
                        "type": "entry",
                        "symbol": sym,
                        "direction": direction,
                        "entry_price": str(price),
                        "R": str(R),
                        "risk_per_unit": str(risk_per_unit),
                        "risk_usd": str(risk_usd),
                        "tp_levels": [str(tp) for tp in tp_levels],
                        "sl_level": str(sl_level),
                        "notional_usd": str(notional),
                        "qty": str(qty),
                        "equity_at_entry": str(equity),
                        "adx_1h": str(adx_val),
                        "vol_z_5m": str(vol_z),
                        "sub_label": SUB_LABEL,
                    })

                    open_positions[sym] = {"symbol": sym}
                    if len(open_positions) >= max_open_trades:
                        print("[MAIN] Reached max trades for this equity tier, stopping new entries this loop.")
                        break

                save_profiles(profiles)
                time.sleep(LOOP_SLEEP_SEC)

            except KeyboardInterrupt:
                print("[MAIN] KeyboardInterrupt, stopping.")
                break
            except Exception as e:
                print(f"[MAIN] loop error: {e}")
                time.sleep(10)

    finally:
        if ws_thread is not None:
            ws_thread.stop()
            print("[MAIN] WS thread stop requested.")


if __name__ == "__main__":
    main_loop()
