# app/core/flashback_common.py
# Flashback shared helpers: Bybit v5 HMAC, Telegram, account state, instruments, sizing utils.

import os
import time
import hmac
import hashlib
import threading
from collections import deque
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Tuple, List, Optional, Callable
from urllib.parse import urlencode
from pathlib import Path  # for heartbeat file handling

import requests
import orjson
from dotenv import load_dotenv

# NEW: notifier + subs for transfer notifications
from app.core.notifier_bot import get_notifier
from app.core.subs import all_subs as load_subs

# Optional: WS-first market bus (orderbook + trades, fed by ws_switchboard)
try:
    from app.core import market_bus as _market_bus  # type: ignore
except Exception:
    _market_bus = None  # type: ignore

# --------- Load .env ----------
load_dotenv()

# --------- Constants & Config ----------
BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")

# WebSocket URLs (shared across bots)
BYBIT_WS_PUBLIC_URL = os.getenv("BYBIT_WS_PUBLIC_URL", "wss://stream.bybit.com/v5/public")
BYBIT_WS_PRIVATE_URL = os.getenv("BYBIT_WS_PRIVATE_URL", "wss://stream.bybit.com/v5/private")

# API keys (Main)
KEY_READ = os.getenv("BYBIT_MAIN_READ_KEY", "")
SEC_READ = os.getenv("BYBIT_MAIN_READ_SECRET", "")
KEY_TRADE = os.getenv("BYBIT_MAIN_TRADE_KEY", "")
SEC_TRADE = os.getenv("BYBIT_MAIN_TRADE_SECRET", "")
KEY_XFER = os.getenv("BYBIT_MAIN_TRANSFER_KEY", "")
SEC_XFER = os.getenv("BYBIT_MAIN_TRANSFER_SECRET", "")

# Telegram (legacy basic sender; high-volume bots should use notifier_bot)
TG_TOKEN_MAIN = os.getenv("TG_TOKEN_MAIN", "")
TG_CHAT_MAIN = os.getenv("TG_CHAT_MAIN", "")
TG_TOKEN_NOTIF = os.getenv("TG_TOKEN_NOTIF", TG_TOKEN_MAIN)
TG_CHAT_NOTIF = os.getenv("TG_CHAT_NOTIF", TG_CHAT_MAIN)

# TG spam control
TG_MAX_MSGS_PER_30S = int(os.getenv("TG_MAX_MSGS_PER_30S", "8"))  # hard cap per process
_TG_TIMES: deque = deque(maxlen=256)
_TG_LOCK = threading.Lock()

# Policies
MARGIN_MODE_MAIN = os.getenv("MARGIN_MODE_MAIN", "CROSS").upper()
USE_MAX_LEVERAGE = os.getenv("USE_MAX_LEVERAGE", "true").lower() == "true"
GLOBAL_BREAKER = {"on": os.getenv("GLOBAL_BREAKER", "false").lower() == "true"}

# Tiers
TIER_LEVELS = [Decimal(x) for x in os.getenv(
    "TIER_LEVELS", "50,100,250,500,1000,2500,5000,10000,25000"
).split(",")]

TIER1_SIZE_CAP_PCT = Decimal(os.getenv("TIER1_SIZE_CAP_PCT", "30.0"))
TIER2_SIZE_CAP_PCT = Decimal(os.getenv("TIER2_SIZE_CAP_PCT", "22.5"))
TIER3_SIZE_CAP_PCT = Decimal(os.getenv("TIER3_SIZE_CAP_PCT", "15.0"))

TIER1_MAX_CONC = int(os.getenv("TIER1_MAX_CONC", "1"))
TIER2_MAX_CONC = int(os.getenv("TIER2_MAX_CONC", "2"))
TIER3_MAX_CONC = int(os.getenv("TIER3_MAX_CONC", "3"))

# MMR guard
MMR_TRIM_TRIGGER = Decimal(os.getenv("MMR_TRIM_TRIGGER", "75.0"))
MMR_TRIM_PCT = Decimal(os.getenv("MMR_TRIM_PCT", "33.0"))
MMR_TRIM_MAX_ROUNDS = int(os.getenv("MMR_TRIM_MAX_ROUNDS", "2"))
MMR_BREAKER_ON_FAIL = os.getenv("MMR_BREAKER_ON_FAIL", "true").lower() == "true"

# Exit ladder / reachability
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_MULT = Decimal(os.getenv("ATR_MULT", "1.0"))
R_MIN_TICKS = int(os.getenv("R_MIN_TICKS", "3"))
TP5_MAX_ATR_MULT = Decimal(os.getenv("TP5_MAX_ATR_MULT", "3.0"))
TP5_MAX_PCT = Decimal(os.getenv("TP5_MAX_PCT", "6.0"))

# TP spacing controls (updated default: 3x gaps)
TP_SPACING_MODE = os.getenv("TP_SPACING_MODE", "geometric").lower()  # geometric | linear
TP_BASE_MULT_ATR = Decimal(os.getenv("TP_BASE_MULT_ATR", "0.5"))
TP_SPACING_FACTOR = Decimal(os.getenv("TP_SPACING_FACTOR", "3.0"))  # each gap = prev_gap * 3.0
TP_MIN_TICKS = int(os.getenv("TP_MIN_TICKS", "3"))
TP_TAG_PREFIX = os.getenv("TP_TAG_PREFIX", "FBTP_")  # orderLinkId prefix

# Drip/Sweep
# DRIP_PCT:
#   - "10"   -> 10% (preferred default)
#   - "0.10" -> also 10% (handled by equity_drip_bot._effective_drip_pct)
DRIP_PCT = Decimal(os.getenv("DRIP_PCT", "10"))
# Legacy knobs kept for future sweep logic; equity_drip_bot v2.2 ignores them.
DRIP_MIN_USD = Decimal(os.getenv("DRIP_MIN_USD", "0"))
MAIN_BAL_FLOOR_USD = Decimal(os.getenv("MAIN_BAL_FLOOR_USD", "0"))
SUB_UIDS_ROUND_ROBIN = os.getenv("SUB_UIDS_ROUND_ROBIN", "")
SWEEP_ALLOCATION = os.getenv("SWEEP_ALLOCATION", "60:MAIN,25:FUNDING,15:SUBS")
SWEEP_CUTOFF_TZ = os.getenv("SWEEP_CUTOFF_TZ", "Europe/London")
SWEEP_CUTOFF_HHMM = os.getenv("SWEEP_CUTOFF_HHMM", "23:59")

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12"))
RETRY_BACKOFFS = [0.5, 1.0, 2.0]  # seconds

# Heartbeat file for bot liveness tracking
HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", ".state/heartbeats.json")
_HEARTBEAT_LOCK = threading.Lock()

# Error de-duplication state for alert_bot_error
_ERROR_LAST: Dict[str, Tuple[str, float]] = {}


# --------- Telegram (legacy simple sender, now throttled) ----------

def _tg_rate_limited() -> bool:
    """
    Simple global rate limit:
      - sliding 30s window
      - at most TG_MAX_MSGS_PER_30S sends per process
    """
    if TG_MAX_MSGS_PER_30S <= 0:
        return True
    now = time.time()
    window = 30.0
    with _TG_LOCK:
        while _TG_TIMES and (now - _TG_TIMES[0]) > window:
            _TG_TIMES.popleft()
        if len(_TG_TIMES) >= TG_MAX_MSGS_PER_30S:
            return True
        _TG_TIMES.append(now)
        return False


def send_tg(text: str, main: bool = False) -> None:
    """
    Best-effort Telegram sender:
      - rate limited
      - NEVER raises (network/TG issues are swallowed)
    """
    token = TG_TOKEN_MAIN if main else TG_TOKEN_NOTIF
    chat = TG_CHAT_MAIN if main else TG_CHAT_NOTIF
    if not token or not chat:
        return

    if _tg_rate_limited():
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "disable_web_page_preview": True},
            timeout=6,
        )
    except Exception:
        return


# --------- Standardized bot error alert helper ----------

def alert_bot_error(bot_name: str, error: Any, severity: str = "WARN") -> None:
    """
    Lightweight, shared error → Telegram helper for bots.

    Anti-spam:
      - deduplicates identical (bot_name, severity, error) for 60s
      - still subject to global TG rate limiting
    """
    try:
        msg_text = str(error)
        key = f"{bot_name}:{severity}"
        now = time.time()

        prev = _ERROR_LAST.get(key)
        if prev is not None:
            last_msg, last_ts = prev
            if last_msg == msg_text and (now - last_ts) < 60.0:
                return

        _ERROR_LAST[key] = (msg_text, now)
        msg = f"⚠️ [{bot_name}] ({severity}) {msg_text}"
        send_tg(msg)
    except Exception:
        return


# --------- Bot heartbeat helper ----------

def record_heartbeat(bot_name: str) -> None:
    """
    Record a lightweight heartbeat for a bot into HEARTBEAT_FILE.
    """
    try:
        path = Path(HEARTBEAT_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)

        with _HEARTBEAT_LOCK:
            if path.exists():
                try:
                    raw = path.read_bytes()
                    data = orjson.loads(raw) if raw else {}
                    if not isinstance(data, dict):
                        data = {}
                except Exception:
                    data = {}
            else:
                data = {}

            data[str(bot_name)] = int(time.time())
            path.write_bytes(orjson.dumps(data))
    except Exception:
        return


# --------- Shared Bybit v5 WebSocket auth builder ----------

def build_ws_auth_payload(api_key: str, api_secret: str) -> Dict[str, Any]:
    """
    Build Bybit v5 private WebSocket auth payload.
    """
    if not api_key or not api_secret:
        raise RuntimeError("Missing API key/secret for WS auth")

    expires = int((time.time() + 1) * 1000)
    msg = f"GET/realtime{expires}"
    sig = hmac.new(
        api_secret.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return {
        "op": "auth",
        "args": [api_key, expires, sig],
    }


def build_ws_auth_payload_main() -> Dict[str, Any]:
    """Convenience wrapper for main trading key."""
    return build_ws_auth_payload(KEY_TRADE, SEC_TRADE)


# --------- Time sync against Bybit (fix retCode 10002) ----------

_TIME_OFFSET_MS = 0
_LAST_SYNC_TS = 0.0
_SYNC_LOCK = threading.Lock()
SYNC_INTERVAL_SEC = 300  # 5 minutes


def _now_ms() -> int:
    return int(time.time() * 1000)


def _server_ms_fallback() -> int:
    try:
        r = requests.get(BYBIT_BASE + "/v5/market/time", timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        js = r.json()
        res = js.get("result", {}) or {}
        if "timeNano" in res:
            return int(int(res["timeNano"]) / 1_000_000)
        if "timeSecond" in res:
            return int(res["timeSecond"]) * 1000
    except Exception:
        pass
    return _now_ms()


def sync_time(force: bool = False) -> None:
    global _TIME_OFFSET_MS, _LAST_SYNC_TS
    with _SYNC_LOCK:
        now = time.time()
        if not force and (now - _LAST_SYNC_TS) < SYNC_INTERVAL_SEC:
            return
        srv = _server_ms_fallback()
        loc = _now_ms()
        _TIME_OFFSET_MS = srv - loc
        _LAST_SYNC_TS = now


def _ts() -> str:
    sync_time(False)
    return str(_now_ms() + _TIME_OFFSET_MS)


# --------- Low-level HMAC helpers (Bybit v5) ----------

def _sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def _headers(key: str, secret: str, *, query: str = "", body: str = "") -> Dict[str, str]:
    """
    Bybit v5 HMAC: sign (timestamp + api_key + recv_window + (body or query)).
    """
    ts = _ts()
    rw = "20000"
    payload = ts + key + rw + (body if body else query)
    sig = _sign(secret, payload)
    return {
        "X-BAPI-API-KEY": key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": rw,
        "X-BAPI-SIGN": sig,
        "X-BAPI-SIGN-TYPE": "2",
        "Content-Type": "application/json",
    }


def _check_json_ok(resp_json: Dict[str, Any], url: str) -> Dict[str, Any]:
    if "retCode" in resp_json:
        code = resp_json.get("retCode", 0)
        if code != 0:
            msg = resp_json.get("retMsg", "unknown error")
            raise requests.HTTPError(f"Bybit retCode {code}: {msg} ({url})")
    return resp_json


def _with_retries(fn: Callable[[], requests.Response]) -> requests.Response:
    """
    Generic HTTP retry wrapper.
    Also detects Bybit retCode 10002 and triggers sync_time().
    """
    last_exc: Optional[Exception] = None
    for _i, backoff in enumerate([0.0] + RETRY_BACKOFFS):
        if backoff:
            time.sleep(backoff)
        try:
            r = fn()
            try:
                js = r.json()
                if isinstance(js, dict) and js.get("retCode") == 10002:
                    sync_time(True)
                    continue
            except Exception:
                pass
            r.raise_for_status()
            return r
        except requests.HTTPError as e:
            last_exc = e
            try:
                js = e.response.json()
                if js.get("retCode") == 10002:
                    sync_time(True)
                    continue
            except Exception:
                pass
        except Exception as e:
            last_exc = e
    if last_exc:
        raise last_exc
    raise RuntimeError("request failed without exception (unexpected)")


# --------- Bybit GET/POST with correct v5 signing ----------

def bybit_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    key: str = KEY_READ,
    secret: str = SEC_READ,
    auth: bool = True,
) -> Dict[str, Any]:
    """
    Authenticated GET with correct v5 signing.
    """
    params = params or {}

    # If linear category but missing discriminator, default settleCoin
    if params.get("category") == "linear" and not any(k in params for k in ("symbol", "settleCoin", "baseCoin")):
        params["settleCoin"] = "USDT"

    if auth:
        url_holder: Dict[str, str] = {"url": BYBIT_BASE + path}

        def _call() -> requests.Response:
            if params:
                qs = urlencode(sorted(params.items()))
                url = f"{BYBIT_BASE}{path}?{qs}"
            else:
                qs = ""
                url = f"{BYBIT_BASE}{path}"

            url_holder["url"] = url
            headers = _headers(key, secret, query=qs)
            return requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)

        r = _with_retries(_call)
        url_used = url_holder["url"]
    else:
        def _call_no_auth() -> requests.Response:
            return requests.get(f"{BYBIT_BASE}{path}", params=params, timeout=HTTP_TIMEOUT)

        r = _with_retries(_call_no_auth)
        url_used = f"{BYBIT_BASE}{path}"

    try:
        js = r.json()
    except Exception:
        raise requests.HTTPError(f"Non-JSON response from Bybit: {url_used}")
    return _check_json_ok(js, url_used)


def bybit_post(
    path: str,
    body: Optional[Dict[str, Any]] = None,
    key: str = KEY_TRADE,
    secret: str = SEC_TRADE,
) -> Dict[str, Any]:
    """
    Authenticated POST:
      - Signs the exact JSON body string used in the request.
    """
    body = body or {}
    url = f"{BYBIT_BASE}{path}"

    def _call() -> requests.Response:
        data = orjson.dumps(body).decode()
        headers = _headers(key, secret, body=data)
        return requests.post(url, data=data, headers=headers, timeout=HTTP_TIMEOUT)

    r = _with_retries(_call)

    try:
        js = r.json()
    except Exception:
        raise requests.HTTPError(f"Non-JSON response from Bybit: {url}")
    return _check_json_ok(js, url)


# --------- Account / Positions / MMR ----------

def get_equity_usdt() -> Decimal:
    res = bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    for acc in res.get("result", {}).get("list", []) or []:
        for coin in acc.get("coin", []) or []:
            if coin.get("coin") == "USDT":
                return Decimal(str(coin.get("equity", "0")))
    return Decimal("0")


def get_mmr_pct() -> Decimal:
    res = bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    lst = res.get("result", {}).get("list", []) or []
    if not lst:
        return Decimal("0")
    try:
        return Decimal(str(lst[0].get("marginRatio", "0"))) * Decimal("100")
    except Exception:
        return Decimal("0")


def list_open_positions(category: str = "linear", settle_coin: str = "USDT") -> List[Dict[str, Any]]:
    """
    List open positions for the given category.

    Backwards compatible:
      - older callers: list_open_positions()
      - new callers:   list_open_positions(category="linear")
    """
    params: Dict[str, Any] = {"category": category}
    if category == "linear":
        params["settleCoin"] = settle_coin

    res = bybit_get("/v5/position/list", params)
    rows = res.get("result", {}).get("list", []) or []
    out: List[Dict[str, Any]] = []
    for p in rows:
        try:
            if Decimal(str(p.get("size", "0"))) > 0:
                out.append(p)
        except Exception:
            pass
    return out


# --------- Orders / Open orders ----------

def list_open_orders(symbol: Optional[str] = None) -> List[dict]:
    params: Dict[str, Any] = {"category": "linear"}
    if symbol:
        params["symbol"] = symbol
    else:
        params["settleCoin"] = "USDT"
    r = bybit_get("/v5/order/realtime", params)
    return r.get("result", {}).get("list", []) or []


def list_symbol_tp_orders(symbol: str, side_now: str) -> List[dict]:
    """Return open reduce-only limit orders (TPs) for symbol and current side."""
    opp = "Sell" if side_now.lower() == "buy" else "Buy"
    out: List[dict] = []
    for o in list_open_orders(symbol):
        try:
            if (
                o.get("orderType") == "Limit"
                and str(o.get("reduceOnly", "False")).lower() == "true"
                and o.get("side") == opp
            ):
                out.append(o)
        except Exception:
            pass
    return out


# --------- Instruments / Market data ----------

_INSTR_CACHE: Dict[str, Dict[str, Decimal]] = {}


def get_ticks(symbol: str) -> Tuple[Decimal, Decimal, Decimal]:
    hit = _INSTR_CACHE.get(symbol)
    if hit:
        return hit["tick"], hit["step"], hit["min_notional"]

    r = bybit_get("/v5/market/instruments-info",
                  {"category": "linear", "symbol": symbol},
                  auth=False)
    it = (r.get("result", {}) or {}).get("list", [{}])
    it = it[0] if it else {}
    tick = Decimal(str(((it.get("priceFilter") or {}).get("tickSize") or "0.01")))
    step = Decimal(str(((it.get("lotSizeFilter") or {}).get("qtyStep") or "0.001")))
    min_notional = Decimal("5")
    _INSTR_CACHE[symbol] = {"tick": tick, "step": step, "min_notional": min_notional}
    return tick, step, min_notional


def last_price(symbol: str) -> Decimal:
    """
    REST-based last traded price from /v5/market/tickers.
    Kept as the lowest-level primitive (used as fallback by WS-first helpers).
    """
    r = bybit_get("/v5/market/tickers", {"category": "linear", "symbol": symbol}, auth=False)
    lst = (r.get("result", {}) or {}).get("list", []) or []
    if not lst:
        return Decimal("0")
    return Decimal(str(lst[0].get("lastPrice", "0")))


# --------- WS-first price helpers (using market_bus if available) ----------

def best_bid_ask_ws_first(symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    WS-first best bid/ask:
      - Uses market_bus.best_bid_ask(symbol) if available and non-empty.
      - Falls back to (None, None) if WS data missing.
    """
    if _market_bus is None:
        return None, None
    try:
        bid, ask = _market_bus.best_bid_ask(symbol)
        # market_bus already returns Decimals, but normalize defensively
        bid_dec = Decimal(str(bid)) if bid is not None else None
        ask_dec = Decimal(str(ask)) if ask is not None else None
        return bid_dec, ask_dec
    except Exception:
        return None, None


def mid_price_ws_first(symbol: str) -> Optional[Decimal]:
    """
    WS-first mid price:
      - Uses market_bus.mid_price(symbol) if available.
      - Returns None if WS data missing.
    """
    if _market_bus is None:
        return None
    try:
        m = _market_bus.mid_price(symbol)
        return Decimal(str(m)) if m is not None else None
    except Exception:
        return None


def spread_bps_ws(symbol: str) -> Optional[Decimal]:
    """
    WS-first spread in bps (spread / mid * 10_000), or None.
    """
    if _market_bus is None:
        return None
    try:
        s = _market_bus.spread_bps(symbol)
        return Decimal(str(s)) if s is not None else None
    except Exception:
        return None


def last_price_ws_first(symbol: str) -> Decimal:
    """
    Preferred price accessor for the ecosystem:

      - Try WS mid-price from market_bus (fast, real-time, no REST).
      - If missing/stale or error, fall back to REST last_price().

    This is what new bots / AI logic should use instead of `last_price`.
    """
    ws_mid = mid_price_ws_first(symbol)
    if ws_mid is not None and ws_mid > 0:
        return ws_mid
    return last_price(symbol)


def _kline(symbol: str, interval: str, limit: int) -> List[List[str]]:
    r = bybit_get("/v5/market/kline",
                  {"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)},
                  auth=False)
    return list(reversed((r.get("result", {}) or {}).get("list", []) or []))


def atr14(symbol: str, interval: str = "240", limit: int = 100) -> Decimal:
    rows = _kline(symbol, interval, limit)
    if len(rows) < 15:
        return Decimal("0")
    trs: List[Decimal] = []
    prev_close = Decimal(str(rows[0][4]))
    for i in range(1, len(rows)):
        high = Decimal(str(rows[i][2]))
        low = Decimal(str(rows[i][3]))
        close = Decimal(str(rows[i][4]))
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    if len(trs) < 14:
        return Decimal("0")
    return sum(trs[-14:]) / Decimal("14")


# --------- Rounding / Qty helpers ----------

def qdown(x: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return x
    return (x / step).quantize(Decimal("0"), rounding=ROUND_DOWN) * step


def psnap(x: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return x
    return (x / tick).quantize(Decimal("0"), rounding=ROUND_DOWN) * tick


def pct(val: Decimal, p: Decimal) -> Decimal:
    return (val * p / Decimal(100))


# --------- Tier helpers ----------

def tier_from_equity(eq: Decimal) -> Tuple[int, int]:
    level = 1
    for i, th in enumerate(TIER_LEVELS, start=1):
        level = i
        if eq < th:
            break
    if level <= 3:
        return 1, level
    elif level <= 6:
        return 2, level
    else:
        return 3, level


def cap_pct_for_tier(tier: int) -> Decimal:
    return {1: TIER1_SIZE_CAP_PCT, 2: TIER2_SIZE_CAP_PCT, 3: TIER3_SIZE_CAP_PCT}[tier]


def max_conc_for_tier(tier: int) -> int:
    return {1: TIER1_MAX_CONC, 2: TIER2_MAX_CONC, 3: TIER3_MAX_CONC}[tier]


# --------- Leverage / Margin mode / Orders ----------

def set_cross_margin(symbol: str) -> None:
    try:
        bybit_post("/v5/position/switch-isolated", {"category": "linear", "symbol": symbol, "tradeMode": 0})
    except Exception:
        pass


def set_symbol_leverage(symbol: str, lev: int) -> None:
    try:
        bybit_post("/v5/position/set-leverage", {
            "category": "linear", "symbol": symbol, "buyLeverage": str(lev), "sellLeverage": str(lev)
        })
    except Exception:
        pass


def symbol_max_leverage_default(symbol: str) -> int:
    return 25 if USE_MAX_LEVERAGE else 5


def place_market_entry(symbol: str, side: str, qty: Decimal, leverage: Optional[int] = None) -> Dict[str, Any]:
    lev = leverage if leverage is not None else symbol_max_leverage_default(symbol)
    set_cross_margin(symbol)
    set_symbol_leverage(symbol, lev)
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": "Buy" if side.upper() == "LONG" else "Sell",
        "orderType": "Market",
        "qty": str(qty),
        "positionIdx": 0
    }
    return bybit_post("/v5/order/create", body)


def reduce_only_market(symbol: str, side: str, qty: Decimal) -> Dict[str, Any]:
    opp = "Sell" if side.lower() == "buy" else "Buy"
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": opp,
        "orderType": "Market",
        "qty": str(qty),
        "reduceOnly": True,
        "positionIdx": 0
    }
    return bybit_post("/v5/order/create", body)


def set_stop_loss(symbol: str, sl_price: Decimal) -> None:
    bybit_post("/v5/position/trading-stop", {
        "category": "linear",
        "symbol": symbol,
        "stopLoss": str(sl_price),
        "slTriggerBy": "LastPrice"
    })


def cancel_all(symbol: str) -> None:
    bybit_post("/v5/order/cancel-all", {"category": "linear", "symbol": symbol})


def place_reduce_tp(
    symbol: str,
    side_now: str,
    qty: Decimal,
    price: Decimal,
    *,
    link_id: Optional[str] = None
) -> Dict[str, Any]:
    tp_side = "Sell" if side_now.lower() == "buy" else "Buy"
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": tp_side,
        "orderType": "Limit",
        "qty": str(qty),
        "price": str(price),
        "reduceOnly": True
    }
    if link_id:
        body["orderLinkId"] = link_id
    return bybit_post("/v5/order/create", body)


# --------- TP ladder helpers (STABLE) ----------

def _base_tp_delta(symbol: str, entry_px: Decimal) -> Decimal:
    tick, _, _ = get_ticks(symbol)
    a = atr14(symbol, interval="60", limit=120)
    min_by_ticks = tick * TP_MIN_TICKS
    return max(a * TP_BASE_MULT_ATR, min_by_ticks)


def calc_tp_prices(symbol: str, side: str, entry_px: Decimal) -> List[Decimal]:
    tick, _, _ = get_ticks(symbol)
    base_gap = _base_tp_delta(symbol, entry_px)

    if TP_SPACING_MODE == "linear":
        gaps = [base_gap] * 5
    else:
        gaps = []
        g = base_gap
        f = max(TP_SPACING_FACTOR, Decimal("1.0"))
        for _ in range(5):
            gaps.append(g)
            g = g * f

    atr_1h = atr14(symbol, interval="60", limit=120)
    cap_by_atr = atr_1h * TP5_MAX_ATR_MULT if atr_1h > 0 else None
    cap_by_pct = entry_px * TP5_MAX_PCT / Decimal(100)

    def _cap_delta(d: Decimal) -> Decimal:
        caps = [cap_by_pct]
        if cap_by_atr is not None:
            caps.append(cap_by_atr)
        return min([c for c in caps if c is not None])

    prices: List[Decimal] = []
    run = entry_px
    for gap in gaps:
        d = _cap_delta(gap)
        if side.upper() == "LONG":
            run = run + d
        else:
            run = run - d
        prices.append(psnap(run, tick))

    min_step = tick * TP_MIN_TICKS
    fixed: List[Decimal] = []
    for i, p in enumerate(prices):
        if i == 0:
            fixed.append(p)
        else:
            if side.upper() == "LONG":
                p = max(p, fixed[-1] + min_step)
            else:
                p = min(p, fixed[-1] - min_step)
            fixed.append(psnap(p, tick))
    return fixed


def split_qty_even(total_qty: Decimal, symbol: str, parts: int = 5) -> List[Decimal]:
    _, step, _ = get_ticks(symbol)
    if parts <= 1:
        return [qdown(total_qty, step)]
    leg = qdown(total_qty / Decimal(parts), step)
    out = [leg] * (parts - 1)
    used = leg * (parts - 1)
    out.append(qdown(total_qty - used, step))
    return out


def ensure_tp_ladder_stable(symbol: str, side_now: str, entry_px: Decimal, total_qty: Decimal) -> None:
    """
    Idempotent, STABLE ladder:
      - Creates missing TP legs tagged with orderLinkId = TP_TAG_PREFIX + idx (1..5).
      - NEVER cancels or resizes existing TP legs just because one filled.
    """
    prices = calc_tp_prices(symbol, "LONG" if side_now.lower() == "buy" else "SHORT", entry_px)
    qtys = split_qty_even(total_qty, symbol, parts=5)
    desired = {f"{TP_TAG_PREFIX}{i+1}": (qtys[i], prices[i]) for i in range(5)}

    existing = list_symbol_tp_orders(symbol, "buy" if side_now.lower() == "sell" else "sell")
    existing_by_link: Dict[str, dict] = {}
    for o in existing:
        link = o.get("orderLinkId") or ""
        if link.startswith(TP_TAG_PREFIX):
            existing_by_link[link] = o

    for i in range(1, 6):
        lid = f"{TP_TAG_PREFIX}{i}"
        if lid not in existing_by_link:
            qty, px = desired[lid]
            try:
                place_reduce_tp(symbol, side_now, qty, px, link_id=lid)
            except Exception as e:
                send_tg(f"[TP/Ladder] place {symbol} leg {i} failed: {e}")


# --------- Internal Transfers (drip) ----------

def inter_transfer_usdt_to_sub(uid: str, amount: Decimal) -> Dict[str, Any]:
    """
    Internal transfer from MAIN unified → sub unified (USDT only),
    with Telegram notifications to main + sub channels.
    """
    body = {
        "transferId": _ts(),
        "coin": "USDT",
        "amount": str(amount),
        "fromAccountType": "UNIFIED",
        "toAccountType": "UNIFIED",
        "toMemberId": str(uid),
    }
    res = bybit_post("/v5/asset/transfer/inter-transfer", body, key=KEY_XFER, secret=SEC_XFER)

    try:
        subs = load_subs()
        sub = next((s for s in subs if str(s.get("uid")) == str(uid)), None)
        label = (sub or {}).get("label", f"uid={uid}")
        channel = (sub or {}).get("channel", label)

        main_tg = get_notifier("main")
        main_tg.info(f"🏦 Internal transfer → {label} ({uid}): {amount} USDT")

        if channel:
            sub_tg = get_notifier(channel)
            sub_tg.info(f"📥 Deposit received from MAIN: {amount} USDT")
    except Exception:
        pass

    return res


# --------- Utility: qty from notional % ----------

def qty_from_pct(symbol: str, equity: Decimal, pct_notional: Decimal) -> Decimal:
    # Prefer WS mid if available; otherwise REST lastPrice.
    price = last_price_ws_first(symbol)
    if price <= 0:
        return Decimal("0")
    _tick, step, _ = get_ticks(symbol)
    notional = (equity * pct_notional / Decimal(100))
    raw_qty = notional / price
    return qdown(raw_qty, step)


# --------- Safety: instrument discovery ----------

def list_linear_usdt_symbols() -> List[str]:
    r = bybit_get("/v5/market/instruments-info", {"category": "linear", "limit": "1000"}, auth=False)
    return [
        x["symbol"] for x in (r.get("result", {}) or {}).get("list", []) or []
        if x.get("quoteCoin") == "USDT" and x.get("status") == "Trading"
    ]
