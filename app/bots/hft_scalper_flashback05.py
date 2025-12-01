#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — HFT Scalper (flashback05) v1.3

Account binding:
    - Trades ONLY on the Bybit subaccount whose keys are in:
        BYBIT_FLASHBACK05_API_KEY
        BYBIT_FLASHBACK05_API_SECRET

Integration points:
- Uses shared state/market_data_bus.json if present (written by market_data_bus.py).
- Falls back to REST tickers/orderbook if bus is missing or stale.
- Uses app.core.portfolio_guard.can_open_trade(...) as the final risk gate.
- Logs trade-open feature snapshots to state/features_trades.jsonl via feature_store.log_trade_open.
- Sends notifications via notifier_bot channel "flashback05".

Core behavior:
- High-frequency-ish scalper (loop ~1s) on a small, liquid symbol universe.
- Trades micro bursts based on:
    • short-term mid-price move
    • orderbook imbalance
    • spread and depth sanity checks
- 1% of equity per trade, capped by portfolio_guard.
- Cross margin + MAX leverage per symbol (same pattern as EMA fb03 bot).
- 3 TP limit orders (reduce-only) & 1 SL (stop-market) in LIVE mode:
    • TP1  = 0.4R
      TP2  = 0.8R
      TP3  = 1.2R
      SL   = 0.6R
    (R = ATR(5m) * ATR_MULT_R_HFT)

Simulation:
- If HFT_FB05_LIVE=0 and HFT_FB05_SIM_EQUITY>0:
    • Uses sim equity for sizing
    • DOES NOT place real orders
    • Still logs trade-open features with mode="PAPER"

Safety:
- Requires env HFT_FB05_LIVE=1 to actually place orders.
"""

from __future__ import annotations

import json
import os
import time
import hmac
import hashlib
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.core.logger import get_logger
from app.core.notifier_bot import get_notifier
from app.core.feature_store import log_trade_open

# Portfolio guard is optional but strongly recommended
try:
    from app.core import portfolio_guard  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    portfolio_guard = None  # type: ignore[assignment]

log = get_logger("hft_scalper_fb05")

# ---------------------------------------------------------------------------
# Paths & env
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

ENV_PATH = ROOT / ".env"
if ENV_PATH.exists():
    from dotenv import load_dotenv

    load_dotenv(ENV_PATH)
    log.info("[HFT fb05] Loaded .env from %s", ENV_PATH)
else:
    log.warning("[HFT fb05] .env not found at %s; using OS env only.", ENV_PATH)

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")
BYBIT_RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "20000")
ACCOUNT_TYPE = "UNIFIED"
CATEGORY = "linear"

# *** BOUND TO FLASHBACK05 SUBACCOUNT ***
API_KEY = os.getenv("BYBIT_FLASHBACK05_API_KEY", "")
API_SECRET = os.getenv("BYBIT_FLASHBACK05_API_SECRET", "")

if not API_KEY or not API_SECRET:
    log.warning(
        "BYBIT_FLASHBACK05_API_KEY / _SECRET missing in env. "
        "HFT bot will NOT place real orders until set."
    )

SUB_LABEL = "flashback05"
SUB_UID = os.getenv("SUB_UID_5", "524637467")  # default from your mapping
STRATEGY_NAME = "Sub5_HFT"    # must match strategies.yaml

# Live switch: require explicit opt-in to send live orders
HFT_LIVE = os.getenv("HFT_FB05_LIVE", "").strip().lower() in ("1", "true", "yes", "y", "on")

# Sim equity for DRY-RUN / PAPER mode (still logs features)
SIM_EQUITY = Decimal(os.getenv("HFT_FB05_SIM_EQUITY", "0"))

# Symbol universe for this bot (keep in sync with strategies.yaml -> Sub5_HFT)
HFT_SYMBOLS = [
    "SOLUSDT",
    "LINKUSDT",
    "INJUSDT",
    "OPUSDT",
    "AVAXUSDT",
    "MATICUSDT",
    "NEARUSDT",
    "ARBUSDT",
    "HBARUSDT",
]

# Risk / sizing
RISK_ALLOC_PCT = Decimal(os.getenv("HFT_FB05_RISK_PCT", "0.01"))  # default 1% of equity

# ATR / R settings (for micro scalps)
ATR_PERIOD_LTF = 14
ATR_MULT_R_HFT = Decimal(os.getenv("HFT_FB05_ATR_MULT", "0.50"))  # a bit tighter than swing

# Micro signal thresholds
MID_MOVE_THRESHOLD_PCT = Decimal(os.getenv("HFT_FB05_MOVE_PCT", "0.10"))  # 0.10% move vs last mid
IMBALANCE_MIN = Decimal(os.getenv("HFT_FB05_IMB_MIN", "0.60"))            # 60%+ of top-5 depth one side

# Spread & depth guard
MAX_SPREAD_PCT = Decimal(os.getenv("HFT_FB05_MAX_SPREAD", "0.10"))  # 0.10%
DEPTH_MULTIPLIER = Decimal(os.getenv("HFT_FB05_DEPTH_MULT", "2"))

# TP / SL structure (R multiples)
from typing import List as _List  # avoid confusion with above
TP_MULTS: _List[Decimal] = [
    Decimal("0.4"),
    Decimal("0.8"),
    Decimal("1.2"),
]
SL_MULT = Decimal("0.6")  # stop closer than full 1R for fast cut

NUM_TPS = len(TP_MULTS)

# Loop cadence & market bus
LOOP_SLEEP_SEC = float(os.getenv("HFT_FB05_LOOP_SEC", "1.0"))
MARKET_BUS_PATH = STATE_DIR / "market_data_bus.json"
MARKET_BUS_STALE_MS = int(os.getenv("HFT_MARKET_BUS_STALE_MS", "2000"))

# Telegram notifier
tg = get_notifier(SUB_LABEL)

# Local mid-price memory
_last_mid: Dict[str, Decimal] = {}

# ---------------------------------------------------------------------------
# HTTP signing / basic Bybit request
# ---------------------------------------------------------------------------


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
    timeout: float = 8.0,
) -> Dict[str, Any]:
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Bybit API key/secret is not configured for flashback05.")

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

    method_u = method.upper()
    if method_u == "GET":
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    else:
        resp = requests.post(url, params=params, data=body_str, headers=headers, timeout=timeout)

    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") not in (0, "0"):
        raise RuntimeError(f"Bybit error {data.get('retCode')}: {data.get('retMsg')}")
    return data


# ---------------------------------------------------------------------------
# Account helpers
# ---------------------------------------------------------------------------


def get_equity_usdt() -> Decimal:
    try:
        data = bybit_request(
            "GET",
            "/v5/account/wallet-balance",
            {"accountType": ACCOUNT_TYPE, "coin": "USDT"},
        )
        lst = data.get("result", {}).get("list", []) or []
        if not lst:
            return Decimal("0")
        acct = lst[0]
        eq_str = acct.get("totalEquity") or acct.get("totalWalletBalance") or "0"
        return Decimal(str(eq_str))
    except Exception as e:
        log.warning("get_equity_usdt failed: %s", e)
        return Decimal("0")


def get_open_positions() -> Dict[str, Dict[str, Any]]:
    try:
        data = bybit_request(
            "GET",
            "/v5/position/list",
            {"category": CATEGORY, "settleCoin": "USDT"},
        )
        rows = data.get("result", {}).get("list", []) or []
    except Exception as e:
        log.warning("get_open_positions failed: %s", e)
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for p in rows:
        try:
            sym = p.get("symbol")
            if not sym:
                continue
            size = Decimal(str(p.get("size", "0") or "0"))
            if size == 0:
                continue
            out[sym] = p
        except Exception:
            continue
    return out


def get_closed_pnl_latest(sym: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    try:
        data = bybit_request(
            "GET",
            "/v5/position/closed-pnl",
            {"category": CATEGORY, "symbol": sym, "limit": "1"},
        )
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


# ---------------------------------------------------------------------------
# Market data bus + orderbook / ATR
# ---------------------------------------------------------------------------


def load_market_bus() -> Optional[Dict[str, Any]]:
    if not MARKET_BUS_PATH.exists():
        return None
    try:
        raw = MARKET_BUS_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        updated_ms = int(data.get("updated_ms", 0))
        now_ms = int(time.time() * 1000)
        if now_ms - updated_ms > MARKET_BUS_STALE_MS:
            return None
        return data
    except Exception as e:
        log.warning("Failed to load market_data_bus.json: %s", e)
        return None


def get_ticker_and_book_rest(sym: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    ticker: Optional[Dict[str, Any]] = None
    ob: Optional[Dict[str, Any]] = None

    try:
        tdata = bybit_request("GET", "/v5/market/tickers", {"category": CATEGORY, "symbol": sym})
        tlist = tdata.get("result", {}).get("list", []) or []
        if tlist:
            ticker = tlist[0]
    except Exception as e:
        log.warning("[%s] REST ticker failed: %s", sym, e)

    try:
        obdata = bybit_request("GET", "/v5/market/orderbook", {"category": CATEGORY, "symbol": sym, "limit": "50"})
        ob = obdata.get("result", {}) or None
    except Exception as e:
        log.warning("[%s] REST orderbook failed: %s", sym, e)

    return ticker, ob


def parse_orderbook(ob: Dict[str, Any]) -> Tuple[List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]:
    bids_raw = ob.get("b") or ob.get("bid") or []
    asks_raw = ob.get("a") or ob.get("ask") or []

    def _parse(levels: List[List[str]]) -> List[Tuple[Decimal, Decimal]]:
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

    return _parse(bids_raw), _parse(asks_raw)


def fetch_klines_5m(sym: str, limit: int = 100) -> List[Dict[str, Any]]:
    data = bybit_request(
        "GET",
        "/v5/market/kline",
        {"category": CATEGORY, "symbol": sym, "interval": "5", "limit": str(limit)},
    )
    rows = data.get("result", {}).get("list", []) or []
    rows = list(reversed(rows))
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "start": int(r[0]),
                "open": Decimal(r[1]),
                "high": Decimal(r[2]),
                "low": Decimal(r[3]),
                "close": Decimal(r[4]),
            }
        )
    return out


def atr_from_5m(candles: List[Dict[str, Any]], period: int) -> Decimal:
    if len(candles) < period + 1:
        return Decimal("0")
    trs: List[Decimal] = []
    for i in range(1, len(candles)):
        c = candles[i]
        p = candles[i - 1]
        high = c["high"]
        low = c["low"]
        prev_close = p["close"]
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


# ---------------------------------------------------------------------------
# Leverage helpers (max leverage + cross)
# ---------------------------------------------------------------------------


def get_instrument_info(sym: str) -> Optional[Dict[str, Any]]:
    try:
        data = bybit_request(
            "GET",
            "/v5/market/instruments-info",
            {"category": CATEGORY, "symbol": sym},
        )
        lst = data.get("result", {}).get("list", []) or []
        return lst[0] if lst else None
    except Exception as e:
        log.warning("[%s] instruments-info failed: %s", sym, e)
        return None


def get_symbol_max_leverage(inst: Dict[str, Any]) -> str:
    lev_filter = inst.get("leverageFilter") or {}
    max_lev = lev_filter.get("maxLeverage") or lev_filter.get("maxLeverageE")
    if max_lev in (None, "", "0"):
        return "50"
    return str(max_lev)


def ensure_cross_max_leverage(sym: str, inst: Dict[str, Any]) -> None:
    max_lev = get_symbol_max_leverage(inst)

    try:
        body_mode = {
            "category": CATEGORY,
            "symbol": sym,
            "tradeMode": 0,  # 0 = cross
            "buyLeverage": max_lev,
            "sellLeverage": max_lev,
        }
        bybit_request("POST", "/v5/position/switch-isolated", body=body_mode)
        log.info("[%s] switched to CROSS margin, lev=%sx", sym, max_lev)
    except Exception as e:
        log.warning("[%s] switch-isolated failed: %s", sym, e)

    try:
        body_lev = {
            "category": CATEGORY,
            "symbol": sym,
            "buyLeverage": max_lev,
            "sellLeverage": max_lev,
        }
        bybit_request("POST", "/v5/position/set-leverage", body=body_lev)
        log.info("[%s] set-leverage -> %sx", sym, max_lev)
    except Exception as e:
        log.warning("[%s] set-leverage failed: %s", sym, e)


# ---------------------------------------------------------------------------
# Signal logic: mid move + imbalance + spread/depth
# ---------------------------------------------------------------------------


def compute_mid_and_imbalance(
    bids: List[Tuple[Decimal, Decimal]],
    asks: List[Tuple[Decimal, Decimal]],
    depth_levels: int = 5,
) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    if not bids or not asks:
        return None, None
    best_bid, _ = bids[0]
    best_ask, _ = asks[0]
    mid = (best_bid + best_ask) / Decimal("2")
    if mid <= 0:
        return None, None

    depth_b = sum(p * q for p, q in bids[:depth_levels])
    depth_a = sum(p * q for p, q in asks[:depth_levels])
    total = depth_b + depth_a
    if total <= 0:
        return mid, None

    imbalance = depth_b / total  # ~1 = bid-heavy, ~0 = ask-heavy
    return mid, imbalance


def spread_pct_from_mid(best_bid: Decimal, best_ask: Decimal) -> Decimal:
    mid = (best_bid + best_ask) / Decimal("2")
    if mid <= 0:
        return Decimal("0")
    return (best_ask - best_bid) / mid * Decimal("100")


def decide_signal(sym: str, mid: Decimal, imbalance: Optional[Decimal]) -> Optional[Dict[str, Any]]:
    last_mid = _last_mid.get(sym)
    _last_mid[sym] = mid

    if last_mid is None or mid <= 0 or last_mid <= 0 or imbalance is None:
        return None

    move_pct = (mid - last_mid) / last_mid * Decimal("100")

    direction: Optional[str] = None
    side_reason = ""

    if move_pct >= MID_MOVE_THRESHOLD_PCT and imbalance >= IMBALANCE_MIN:
        direction = "LONG"
        side_reason = "up-move + bid-imbalance"
    elif move_pct <= -MID_MOVE_THRESHOLD_PCT and imbalance <= (Decimal("1") - IMBALANCE_MIN):
        direction = "SHORT"
        side_reason = "down-move + ask-imbalance"

    if direction is None:
        return None

    return {
        "symbol": sym,
        "direction": direction,
        "mid": mid,
        "move_pct": move_pct,
        "imbalance": imbalance,
        "reason": side_reason,
    }


# ---------------------------------------------------------------------------
# Orders: entry + TP/SL bracket
# ---------------------------------------------------------------------------


def place_market_order(sym: str, direction: str, notional_usd: Decimal, price: Decimal) -> Tuple[str, Decimal]:
    side = "Buy" if direction == "LONG" else "Sell"
    qty = (notional_usd / price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if qty <= 0:
        raise RuntimeError("qty <= 0 (equity too small / price too high)")

    body = {
        "category": CATEGORY,
        "symbol": sym,
        "side": side,
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "IOC",
        "reduceOnly": False,
    }
    bybit_request("POST", "/v5/order/create", body=body)
    return side, qty


def cancel_all_orders(sym: str) -> None:
    try:
        bybit_request("POST", "/v5/order/cancel-all", body={"category": CATEGORY, "symbol": sym})
    except Exception as e:
        log.warning("[%s] cancel-all failed: %s", sym, e)


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
        log.warning("[%s] close_position_market failed: %s", sym, e)


def compute_hft_levels(
    direction: str,
    entry_price: Decimal,
    R: Decimal,
) -> Tuple[List[Decimal], Decimal]:
    """
    Pure math: compute TP and SL levels for HFT scalper.
    Used in BOTH live and paper modes (no HTTP here).
    """
    is_long = (direction == "LONG")

    tp_levels: List[Decimal] = []
    for mult in TP_MULTS:
        if is_long:
            tp_levels.append((entry_price + mult * R).quantize(Decimal("0.0001"), rounding=ROUND_DOWN))
        else:
            tp_levels.append((entry_price - mult * R).quantize(Decimal("0.0001"), rounding=ROUND_DOWN))

    if is_long:
        sl_level = (entry_price - SL_MULT * R).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    else:
        sl_level = (entry_price + SL_MULT * R).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

    return tp_levels, sl_level


def place_hft_bracket(
    sym: str,
    direction: str,
    qty: Decimal,
    entry_price: Decimal,
    R: Decimal,
) -> Tuple[List[Decimal], Decimal]:
    """
    LIVE mode only: place 3 TP limit orders and 1 SL stop-market.
    Returns (tp_levels, sl_level).
    """
    is_long = (direction == "LONG")

    tp_levels, sl_level = compute_hft_levels(direction, entry_price, R)

    if is_long:
        tp_side = "Sell"
        sl_trigger_dir = 1  # price falls through
    else:
        tp_side = "Buy"
        sl_trigger_dir = 2  # price rises through

    q_unit = (qty / Decimal(NUM_TPS)).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if q_unit <= 0:
        return tp_levels, sl_level

    chunks: List[Decimal] = [q_unit] * NUM_TPS
    total_chunks = q_unit * Decimal(NUM_TPS)
    remainder = qty - total_chunks
    if remainder > 0:
        chunks[-1] = (chunks[-1] + remainder).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

    for i, (tp_px, q_chunk) in enumerate(zip(tp_levels, chunks), start=1):
        if q_chunk <= 0:
            continue
        body_tp = {
            "category": CATEGORY,
            "symbol": sym,
            "side": tp_side,
            "orderType": "Limit",
            "qty": str(q_chunk),
            "price": str(tp_px),
            "timeInForce": "GTC",
            "reduceOnly": True,
        }
        try:
            bybit_request("POST", "/v5/order/create", body=body_tp)
        except Exception as e:
            log.warning("[%s] TP%d create failed: %s", sym, i, e)

    body_sl = {
        "category": CATEGORY,
        "symbol": sym,
        "side": tp_side if is_long else "Buy",
        "orderType": "Market",
        "timeInForce": "GTC",
        "reduceOnly": True,
        "triggerDirection": sl_trigger_dir,
        "triggerPrice": str(sl_level),
        "qty": str(qty),
        "positionIdx": 0,
    }
    try:
        bybit_request("POST", "/v5/order/create", body=body_sl)
    except Exception as e:
        log.warning("[%s] SL create failed: %s", sym, e)

    return tp_levels, sl_level


# ---------------------------------------------------------------------------
# Core loop
# ---------------------------------------------------------------------------


def main_loop() -> None:
    log.info(
        "=== HFT Scalper v1.3 (%s) === live=%s, risk_pct=%s, sim_equity=%s",
        SUB_LABEL,
        HFT_LIVE,
        RISK_ALLOC_PCT,
        SIM_EQUITY,
    )
    tg.info(
        f"✅ HFT Scalper ONLINE\n"
        f"Sub: {SUB_LABEL}\n"
        f"Live: {HFT_LIVE}\n"
        f"Risk per trade: {RISK_ALLOC_PCT * 100:.2f}% equity\n"
        f"Sim equity: {SIM_EQUITY}"
    )

    while True:
        try:
            real_equity = get_equity_usdt()
            equity = real_equity

            # If real equity is dead but SIM_EQUITY > 0, use sim value
            if equity <= 0 and SIM_EQUITY > 0:
                equity = SIM_EQUITY

            if equity <= 0:
                log.warning("Equity <= 0 (real+sim), sleeping.")
                time.sleep(LOOP_SLEEP_SEC)
                continue

            open_positions = get_open_positions()

            bus = load_market_bus()

            for sym in HFT_SYMBOLS:
                if sym in open_positions:
                    continue  # one position per symbol

                # Market data
                ticker = None
                ob = None
                if bus is not None:
                    sym_block = (bus.get("symbols") or {}).get(sym) or {}
                    ticker = sym_block.get("ticker")
                    ob = sym_block.get("orderbook")

                if ticker is None or ob is None:
                    ticker, ob = get_ticker_and_book_rest(sym)

                if ticker is None or ob is None:
                    continue

                bids, asks = parse_orderbook(ob)
                if not bids or not asks:
                    continue

                best_bid, _ = bids[0]
                best_ask, _ = asks[0]
                spread_pct = spread_pct_from_mid(best_bid, best_ask)
                if spread_pct > MAX_SPREAD_PCT:
                    continue

                mid, imbalance = compute_mid_and_imbalance(bids, asks)
                if mid is None:
                    continue

                notional = (equity * RISK_ALLOC_PCT).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if notional <= 0:
                    continue

                signal = decide_signal(sym, mid, imbalance)
                if not signal:
                    continue

                # Quick depth sanity: depth on aggressed side >= notional * DEPTH_MULTIPLIER
                depth_side = asks if signal["direction"] == "LONG" else bids
                depth_usdt = sum(p * q for p, q in depth_side[:5])
                if depth_usdt < notional * DEPTH_MULTIPLIER:
                    continue

                # ATR / R estimation from 5m
                candles_5m = fetch_klines_5m(sym, limit=80)
                atr_val = atr_from_5m(candles_5m, ATR_PERIOD_LTF)
                if atr_val <= 0:
                    continue
                R = atr_val * ATR_MULT_R_HFT

                # Portfolio guard (if available)
                # Risk ≈ R * SL_MULT * position_size
                position_size = (notional / mid).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
                risk_usd = (R * SL_MULT * position_size).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if risk_usd <= 0:
                    risk_usd = notional.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

                allowed = True
                guard_reason = "ok"
                if portfolio_guard is not None:
                    try:
                        allowed, guard_reason = portfolio_guard.can_open_trade(
                            sub_uid=SUB_UID,
                            strategy_name=STRATEGY_NAME,
                            risk_usd=risk_usd,
                            equity_now_usd=equity,
                        )
                    except Exception as e:
                        log.warning("portfolio_guard.can_open_trade failed: %s", e)

                if not allowed:
                    log.info("[%s] HFT trade blocked by guard: %s", sym, guard_reason)
                    continue

                move_pct = signal["move_pct"]
                imb = signal["imbalance"]
                direction = signal["direction"]
                side = "Buy" if direction == "LONG" else "Sell"

                # Compute TP/SL levels (pure math, works for live & paper)
                tp_levels, sl_level = compute_hft_levels(direction, mid, R)

                trade_mode = "LIVE_CANARY" if HFT_LIVE else "PAPER"

                # Feature store logging happens in BOTH live & paper
                try:
                    log_trade_open(
                        sub_uid=SUB_UID,
                        strategy=STRATEGY_NAME,
                        strategy_id=None,
                        symbol=sym,
                        side=side,
                        mode=trade_mode,
                        equity_usd=equity,
                        risk_usd=risk_usd,
                        risk_pct=(risk_usd / equity * Decimal("100")) if equity > 0 else Decimal("0"),
                        ai_score=float(move_pct),  # crude proxy for "strength"
                        ai_reason=f"mid_move={move_pct:.3f}%, imbalance={imb:.3f}",
                        features={
                            "mid": str(mid),
                            "move_pct": float(move_pct),
                            "imbalance": float(imb),
                            "spread_pct": float(spread_pct),
                            "notional_usd": float(notional),
                            "R": float(R),
                            "atr_5m": float(atr_val),
                            "depth_usdt_side": float(depth_usdt),
                            "risk_usd": float(risk_usd),
                            "hft_live": bool(HFT_LIVE),
                        },
                        signal={
                            "reason": signal["reason"],
                            "timeframe": "HFT",
                            "tf": "HFT",
                            "direction": direction,
                            "mid": str(mid),
                            "move_pct": float(move_pct),
                            "imbalance": float(imb),
                        },
                    )
                except Exception as e:
                    log.warning("feature_store.log_trade_open failed: %s", e)

                # PAPER mode: no real orders, but we log + debug Telegram
                if not HFT_LIVE:
                    tg.debug(
                        f"HFT PAPER {sym} {direction} | mid={mid} | move={move_pct:.3f}% | "
                        f"imbalance={imb:.3f} | spread={spread_pct:.3f}% | R≈{R:.6f} | "
                        f"sim_eq={equity}, notional≈{notional}, risk_usd≈{risk_usd}"
                    )
                    continue

                # LIVE mode: real orders + bracket + Telegram trade summary
                inst = get_instrument_info(sym)
                if inst:
                    ensure_cross_max_leverage(sym, inst)

                try:
                    side_live, qty = place_market_order(sym, direction, notional, mid)
                except Exception as e:
                    msg = f"[{sym}] HFT entry failed: {e}"
                    log.warning(msg)
                    tg.warn(msg)
                    continue

                tp_levels_live, sl_level_live = place_hft_bracket(sym, direction, qty, mid, R)

                # Telegram summary (live)
                msg_lines = [
                    "🟢 HFT ENTRY",
                    f"Sub: {SUB_LABEL}",
                    f"Pair: {sym} ({direction})",
                    f"Mid: {mid}",
                    f"Size: {qty} (notional≈{notional} USDT)",
                    f"Spread: {spread_pct:.3f}%",
                    f"Move: {move_pct:.3f}% | Imb: {imb:.3f}",
                    f"R unit (5m ATR): {R:.6f}",
                    f"SL (≈{SL_MULT}R): {sl_level_live}",
                    "TPs:",
                ]
                for i, tp in enumerate(tp_levels_live, start=1):
                    msg_lines.append(f"  • TP{i}: {tp}")
                msg_lines.append(f"Equity: {equity}")
                msg_lines.append(f"Guard: {guard_reason}")
                msg_lines.append(f"Reason: {signal['reason']}")

                tg.trade("\n".join(msg_lines))

            time.sleep(LOOP_SLEEP_SEC)

        except KeyboardInterrupt:
            log.info("KeyboardInterrupt, stopping HFT loop.")
            break
        except Exception as e:
            log.exception("HFT main_loop error: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    main_loop()
