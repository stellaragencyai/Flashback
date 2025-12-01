#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Car Mission Telegram UI (flashback10) v1.0

Purpose
-------
Manual, high-leverage "car mission" trading UI on top of flashback10.

- Telegram commands:
    /start
    /help
    /status
    /tier
    /long  SYMBOL  NOTIONAL_USD
    /short SYMBOL  NOTIONAL_USD
    /close SYMBOL

- Binds to flashback10 via:
    BYBIT_CAR_MISSION_API_KEY
    BYBIT_CAR_MISSION_API_SECRET

- Enforces:
    • Equity tiers based on mission levels:
        Levels: 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000
        Tier 1 (max 1 trade): levels 25, 50, 100
        Tier 2 (max 2 trades): levels 250, 500, 1000
        Tier 3 (max 3 trades): levels 2500, 5000, 10000, 25000
      (equity below 25 => "pre-mission", max 1 trade)

    • Max concurrent trades per tier.

- Notes:
    - This bot does NOT place TP/SL; your global TP/SL Manager handles exits
      for flashback10 (standard_7 + trailing) as long as strategies.yaml is
      configured appropriately.
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

log = get_logger("car_mission_telebot")

# ---------------------------------------------------------------------------
# Paths & env
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT / ".env"

if ENV_PATH.exists():
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(ENV_PATH)
        log.info("[car_mission] Loaded .env from %s", ENV_PATH)
    except Exception as e:  # pragma: no cover
        log.warning("[car_mission] Failed to load .env: %s", e)
else:
    log.warning("[car_mission] .env not found at %s; using OS env only.", ENV_PATH)

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")
BYBIT_RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "20000")
ACCOUNT_TYPE = "UNIFIED"
CATEGORY = "linear"

API_KEY = os.getenv("BYBIT_CAR_MISSION_API_KEY", "")
API_SECRET = os.getenv("BYBIT_CAR_MISSION_API_SECRET", "")

if not API_KEY or not API_SECRET:
    log.warning("BYBIT_CAR_MISSION_API_KEY / _SECRET missing; bot cannot trade.")

TG_TOKEN = os.getenv("TG_TOKEN_CAR_MISSION", "")
TG_CHAT_ID = os.getenv("TG_CHAT_CAR_MISSION", "")

ALLOWED_USER_ID = os.getenv("CAR_MISSION_ALLOWED_USER_ID", "").strip()

# Allowed symbols (uppercased)
_raw_symbols = os.getenv(
    "CAR_MISSION_SYMBOLS",
    "SOLUSDT,ETHUSDT,FARTCOINUSDT,HBARUSDT,PUMPFUNUSDT,BTCUSDT",
)
ALLOWED_SYMBOLS = {s.strip().upper() for s in _raw_symbols.split(",") if s.strip()}

# Mission levels / tiers
LEVEL_EDGES = [
    Decimal("25"),
    Decimal("50"),
    Decimal("100"),
    Decimal("250"),
    Decimal("500"),
    Decimal("1000"),
    Decimal("2500"),
    Decimal("5000"),
    Decimal("10000"),
    Decimal("25000"),
]


def compute_level_and_tier(equity: Decimal) -> Tuple[int, str, int]:
    """
    Returns (level_index, tier_name, max_trades)

    level_index:
        0 = below 25
        1..10 = edges in LEVEL_EDGES

    Tier mapping:
        pre-mission: eq < 25, max_trades=1
        Tier 1: levels 1..3   (25, 50, 100)          max_trades=1
        Tier 2: levels 4..6   (250, 500, 1000)       max_trades=2
        Tier 3: levels 7..10  (2500, 5k, 10k, 25k)   max_trades=3
    """
    level = 0
    for i, edge in enumerate(LEVEL_EDGES, start=1):
        if equity >= edge:
            level = i

    if level == 0:
        return 0, "pre-mission", 1
    elif 1 <= level <= 3:
        return level, "Tier 1", 1
    elif 4 <= level <= 6:
        return level, "Tier 2", 2
    else:
        return level, "Tier 3", 3


# ---------------------------------------------------------------------------
# Bybit helpers
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
    timeout: float = 10.0,
) -> Dict[str, Any]:
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Car mission Bybit API key/secret not configured.")

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


# ---------------------------------------------------------------------------
# Telegram helpers (simple long-polling)
# ---------------------------------------------------------------------------


def tg_api(method: str, **params: Any) -> Dict[str, Any]:
    if not TG_TOKEN:
        raise RuntimeError("TG_TOKEN_CAR_MISSION not set.")
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        log.info("[TG disabled] %s", text)
        return
    try:
        tg_api("sendMessage", chat_id=TG_CHAT_ID, text=text)
    except Exception as e:
        log.warning("tg_send failed: %s", e)


def user_allowed(message: Dict[str, Any]) -> bool:
    if not ALLOWED_USER_ID:
        return True
    frm = message.get("from") or {}
    uid = str(frm.get("id", ""))
    return uid == ALLOWED_USER_ID


# ---------------------------------------------------------------------------
# Command handling
# ---------------------------------------------------------------------------


def handle_status() -> str:
    eq = get_equity_usdt()
    level, tier_name, max_trades = compute_level_and_tier(eq)
    open_positions = get_open_positions()
    msg = [
        "🚗 Car Mission Status",
        f"Equity: {eq} USDT",
        f"Level index: {level} (edges: {', '.join(str(e) for e in LEVEL_EDGES)})",
        f"Tier: {tier_name} (max trades: {max_trades})",
        f"Open positions: {len(open_positions)}",
    ]
    if open_positions:
        for sym, p in open_positions.items():
            side = p.get("side")
            size = p.get("size")
            entry = p.get("avgPrice")
            msg.append(f"  • {sym} {side} size={size} entry={entry}")
    return "\n".join(msg)


def handle_tier() -> str:
    eq = get_equity_usdt()
    level, tier_name, max_trades = compute_level_and_tier(eq)
    return (
        f"📊 Tier info\n\n"
        f"Equity: {eq} USDT\n"
        f"Level index: {level}\n"
        f"Tier: {tier_name}\n"
        f"Max concurrent trades: {max_trades}"
    )


def handle_entry(direction: str, symbol: str, notional_str: str) -> str:
    symbol = symbol.upper()
    if symbol not in ALLOWED_SYMBOLS:
        return f"❌ Symbol {symbol} is not allowed.\nAllowed: {', '.join(sorted(ALLOWED_SYMBOLS))}"

    try:
        notional = Decimal(notional_str)
    except Exception:
        return "❌ Notional must be a number in USDT."

    if notional <= 0:
        return "❌ Notional must be > 0."

    equity = get_equity_usdt()
    if equity <= 0:
        return "❌ Equity <= 0, cannot trade."

    level, tier_name, max_trades = compute_level_and_tier(equity)
    open_positions = get_open_positions()
    open_count = len(open_positions)

    if open_count >= max_trades:
        return (
            f"⛔ Max trades reached for {tier_name}.\n"
            f"Equity={equity}, Level={level}, Tier={tier_name}, "
            f"Max trades={max_trades}, Current={open_count}"
        )

    # Light sanity: notional cannot exceed equity * 3 (you can adjust this later)
    max_notional_soft = equity * Decimal("3")
    if notional > max_notional_soft:
        return (
            f"⚠️ Requested notional {notional} is extremely high vs equity {equity}.\n"
            f"Soft cap={max_notional_soft}. Adjust your size or tweak the bot later."
        )

    # Fetch a price via tickers
    try:
        data = bybit_request(
            "GET",
            "/v5/market/tickers",
            {"category": CATEGORY, "symbol": symbol},
        )
        lst = data.get("result", {}).get("list", []) or []
        if not lst:
            return f"❌ Could not fetch ticker for {symbol}."
        t = lst[0]
        price = Decimal(str(t.get("lastPrice") or t.get("last_price") or t.get("bid1Price") or "0"))
    except Exception as e:
        log.warning("[%s] ticker failed: %s", symbol, e)
        return f"❌ Failed to fetch ticker for {symbol}: {e}"

    if price <= 0:
        return f"❌ Invalid price for {symbol}."

    # Ensure cross + max leverage
    inst = get_instrument_info(symbol)
    if inst:
        ensure_cross_max_leverage(symbol, inst)

    try:
        side, qty = place_market_order(symbol, direction, notional, price)
    except Exception as e:
        log.warning("[%s] entry failed: %s", symbol, e)
        return f"❌ Entry failed for {symbol}: {e}"

    # Summary
    msg_lines = [
        "🟢 CAR MISSION ENTRY",
        f"Symbol: {symbol} ({direction})",
        f"Side: {side}",
        f"Notional: {notional} USDT",
        f"Qty: {qty}",
        f"Price: {price}",
        f"Equity: {equity}",
        f"Level: {level}, Tier: {tier_name}, Max trades: {max_trades}, Now: {open_count + 1}",
    ]
    msg_lines.append("TP/SL: handled by global TP/SL Manager for flashback10.")
    return "\n".join(msg_lines)


def handle_close(symbol: str) -> str:
    symbol = symbol.upper()
    open_positions = get_open_positions()
    pos = open_positions.get(symbol)
    if not pos:
        return f"ℹ️ No open position found for {symbol}."

    try:
        size = Decimal(str(pos.get("size", "0") or "0"))
    except Exception:
        return f"❌ Could not parse size for {symbol}."

    if size <= 0:
        return f"ℹ️ Position size is already 0 for {symbol}."

    side = pos.get("side", "Buy")
    direction = "LONG" if side == "Buy" else "SHORT"

    try:
        close_position_market(symbol, direction, size)
    except Exception as e:
        log.warning("[%s] close failed: %s", symbol, e)
        return f"❌ Failed to close {symbol}: {e}"

    return f"🟥 Closed {symbol} {direction} position (size={size})."


def handle_help() -> str:
    lines = [
        "🚗 Car Mission Bot — Commands",
        "",
        "/status           — Show equity, level, tier, open positions",
        "/tier             — Show current tier & trade limit",
        "/long  SYMBOL N   — Open LONG with ~N USDT notional",
        "/short SYMBOL N   — Open SHORT with ~N USDT notional",
        "/close SYMBOL     — Close open position on SYMBOL",
        "",
        "Examples:",
        "  /long FARTCOINUSDT 133",
        "  /short SOLUSDT 250",
        "",
        "Tier rules:",
        "  pre-mission (<25): max 1 trade",
        "  Tier 1 (25,50,100): max 1 trade",
        "  Tier 2 (250,500,1000): max 2 trades",
        "  Tier 3 (2500,5000,10000,25000): max 3 trades",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------


def main_loop() -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        log.error("TG_TOKEN_CAR_MISSION / TG_CHAT_CAR_MISSION missing. Exiting.")
        return

    tg_send("✅ Car Mission Telegram bot ONLINE (flashback10).")
    log.info("Car Mission bot started. Allowed symbols: %s", ", ".join(sorted(ALLOWED_SYMBOLS)))

    last_update_id: Optional[int] = None

    while True:
        try:
            params: Dict[str, Any] = {"timeout": 25}
            if last_update_id is not None:
                params["offset"] = last_update_id + 1

            data = tg_api("getUpdates", **params)
            updates = data.get("result", []) or []

            for upd in updates:
                last_update_id = upd.get("update_id", last_update_id)

                message = upd.get("message") or upd.get("edited_message")
                if not message:
                    continue

                chat_id = str(message.get("chat", {}).get("id", ""))
                if TG_CHAT_ID and chat_id != str(TG_CHAT_ID):
                    continue  # ignore other chats

                if not user_allowed(message):
                    log.info("Ignoring message from unauthorized user: %s", message.get("from"))
                    continue

                text = message.get("text") or ""
                text = text.strip()
                if not text.startswith("/"):
                    continue

                parts = text.split()
                cmd = parts[0].lower()

                if cmd in ("/start", "/help"):
                    reply = handle_help()
                elif cmd == "/status":
                    reply = handle_status()
                elif cmd == "/tier":
                    reply = handle_tier()
                elif cmd == "/long" and len(parts) == 3:
                    reply = handle_entry("LONG", parts[1], parts[2])
                elif cmd == "/short" and len(parts) == 3:
                    reply = handle_entry("SHORT", parts[1], parts[2])
                elif cmd == "/close" and len(parts) == 2:
                    reply = handle_close(parts[1])
                else:
                    reply = "❓ Unknown or malformed command. Use /help for usage."

                tg_send(reply)

            time.sleep(1)

        except KeyboardInterrupt:
            log.info("KeyboardInterrupt, stopping car mission bot.")
            break
        except Exception as e:
            log.exception("main_loop error: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    main_loop()
