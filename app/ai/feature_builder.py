# app/ai/feature_builder.py
# Flashback — Feature Builder v1.0
#
# Role:
# - Convert closed trades (from state/trades_log.jsonl) into ML-ready feature rows.
# - For each closed trade:
#     • Pull recent OHLCV candles around entry time.
#     • Compute simple volatility, range, and volume features.
#     • Add timing features (session, day-of-week, hour).
#     • Persist a single row into state/feature_store.jsonl.
#
# This is the backbone for AI training (train_models.py, inference, etc.).
#
# Assumptions:
# - trades_log.jsonl lines look like ClosedTradeRecord from trade_capture_daemon:
#   {
#     "schema_version": 1,
#     "account_label": "main",
#     "symbol": "BTCUSDT",
#     "side": "long",
#     "entry_time_ms": 1763751000000,
#     "exit_time_ms": 1763751300000,
#     "entry_price": "65000.0",
#     "exit_price": "65100.0",
#     "size": "0.01",
#     "gross_pnl": "1.0",
#     "net_pnl": "1.0",
#     "rr": "0",
#     "strategy_id": "UNKNOWN",
#     "setup_tag": "UNLABELED",
#     "meta": {}
#   }

from __future__ import annotations

import json
import math
import time
import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import orjson

try:
    import importlib

    _log_module = importlib.import_module("app.core.logger")
    get_logger = getattr(_log_module, "get_logger")
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


log = get_logger("feature_builder")

# Bybit client for OHLCV
try:
    _core_mod = importlib.import_module("app.core.bybit_client")
    Bybit = getattr(_core_mod, "Bybit")
except Exception:  # pragma: no cover
    Bybit = None  # type: ignore


try:
    cfg_mod = importlib.import_module("app.core.config")
    settings = getattr(cfg_mod, "settings")
except Exception:
    class _DummySettings:  # type: ignore
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore


ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

TRADES_LOG_PATH: Path = STATE_DIR / "trades_log.jsonl"
FEATURE_STORE_PATH: Path = STATE_DIR / "feature_store.jsonl"
FEATURE_CURSOR_PATH: Path = STATE_DIR / "feature_store.cursor"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_feature_cursor() -> int:
    if not FEATURE_CURSOR_PATH.exists():
        return 0
    try:
        return int(FEATURE_CURSOR_PATH.read_text().strip() or "0")
    except Exception:
        return 0


def _save_feature_cursor(pos: int) -> None:
    try:
        FEATURE_CURSOR_PATH.write_text(str(pos))
    except Exception as e:
        log.warning("failed to save feature cursor %s: %r", pos, e)


def _append_feature_row(row: Dict[str, Any]) -> None:
    line = json.dumps(row, separators=(",", ":"))
    with FEATURE_STORE_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _utc_from_ms(ms: int) -> dt.datetime:
    return dt.datetime.utcfromtimestamp(ms / 1000.0).replace(tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------------------
# OHLCV fetch & feature math
# ---------------------------------------------------------------------------

def _get_market_client() -> Any:
    if Bybit is None:
        raise RuntimeError("Bybit client not available (import failed).")
    try:
        # Use "market" profile if you have it; fallback to trade
        return Bybit("market")
    except Exception:
        return Bybit("trade")


def _fetch_ohlcv(
    symbol: str,
    interval: str = "1",
    limit: int = 100,
    end_time_ms: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch OHLCV candles from Bybit v5.

    interval examples: "1" (1m), "3", "5", "15", "60" etc.
    end_time_ms is optional; if provided, used as endTime.
    """
    client = _get_market_client()

    params: Dict[str, Any] = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    resp: Dict[str, Any]
    try:
        # Newer style: named kwargs, v5 kline
        resp = client.get_kline(**params)  # type: ignore[arg-type]
    except TypeError:
        # Fallback older signature, user can adapt if needed
        resp = client.get_kline(symbol, interval, limit)  # type: ignore[call-arg]

    if not isinstance(resp, dict):
        log.warning("get_kline returned non-dict for %s: %r", symbol, resp)
        return []

    result = resp.get("result") or {}
    raw_list = result.get("list") or result.get("klines") or []

    candles: List[Dict[str, Any]] = []
    for row in raw_list:
        # Bybit v5 list typically: [startTime, open, high, low, close, volume, turnover, ...]
        if isinstance(row, list) and len(row) >= 7:
            try:
                ts = int(row[0])
                o = float(row[1])
                h = float(row[2])
                l = float(row[3])
                c = float(row[4])
                v = float(row[5])
            except Exception:
                continue

            candles.append(
                {
                    "ts_ms": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                }
            )
        elif isinstance(row, dict):
            try:
                ts = int(row.get("startTime") or row.get("ts") or row.get("t"))
                o = float(row.get("open") or row.get("o"))
                h = float(row.get("high") or row.get("h"))
                l = float(row.get("low") or row.get("l"))
                c = float(row.get("close") or row.get("c"))
                v = float(row.get("volume") or row.get("v"))
            except Exception:
                continue

            candles.append(
                {
                    "ts_ms": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                }
            )

    # Sort ascending by time
    candles.sort(key=lambda x: x["ts_ms"])
    return candles


def _compute_vol_features(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute simple volatility and volume features from a list of OHLCV candles.
    """
    if not candles:
        return {
            "atr_like": 0.0,
            "atr_pct": 0.0,
            "range_mean": 0.0,
            "range_std": 0.0,
            "volume_zscore": 0.0,
        }

    ranges: List[float] = []
    vols: List[float] = []
    closes: List[float] = []

    for c in candles:
        hi = float(c["high"])
        lo = float(c["low"])
        cl = float(c["close"])
        vol = float(c["volume"])
        ranges.append(hi - lo)
        vols.append(vol)
        closes.append(cl)

    n = len(ranges)
    if n == 0:
        return {
            "atr_like": 0.0,
            "atr_pct": 0.0,
            "range_mean": 0.0,
            "range_std": 0.0,
            "volume_zscore": 0.0,
        }

    range_mean = sum(ranges) / n
    if n > 1:
        range_var = sum((r - range_mean) ** 2 for r in ranges) / (n - 1)
        range_std = math.sqrt(range_var)
    else:
        range_std = 0.0

    last_close = closes[-1]
    atr_like = range_mean
    atr_pct = (atr_like / last_close * 100.0) if last_close > 0 else 0.0

    # Volume z-score for last candle
    vol_mean = sum(vols) / n
    if n > 1:
        vol_var = sum((v - vol_mean) ** 2 for v in vols) / (n - 1)
        vol_std = math.sqrt(vol_var)
    else:
        vol_std = 0.0

    last_vol = vols[-1]
    if vol_std > 0:
        volume_z = (last_vol - vol_mean) / vol_std
    else:
        volume_z = 0.0

    return {
        "atr_like": atr_like,
        "atr_pct": atr_pct,
        "range_mean": range_mean,
        "range_std": range_std,
        "volume_zscore": volume_z,
    }


def _infer_trend(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Crude trend estimate: last close vs simple average of closes.
    """
    if not candles:
        return {"trend_dir": 0, "trend_strength": 0.0}

    closes = [float(c["close"]) for c in candles]
    last = closes[-1]
    mean_close = sum(closes) / len(closes)
    diff = last - mean_close
    if diff > 0:
        trend_dir = 1
    elif diff < 0:
        trend_dir = -1
    else:
        trend_dir = 0

    # normalized diff as "strength"
    trend_strength = diff / mean_close if mean_close > 0 else 0.0

    return {
        "trend_dir": trend_dir,           # -1 = down, 0 = flat, 1 = up
        "trend_strength": trend_strength,
    }


# ---------------------------------------------------------------------------
# Feature construction per trade
# ---------------------------------------------------------------------------

def build_features_for_trade(
    trade: Dict[str, Any],
    *,
    interval: str = "1",
    lookback_candles: int = 100,
) -> Optional[Dict[str, Any]]:
    """
    Build a feature row from a closed trade record.

    interval: Bybit kline interval (string minutes: "1","3","5","15","60"...)
    """
    symbol = str(trade.get("symbol") or "").upper()
    if not symbol:
        return None

    try:
        entry_time_ms = int(trade.get("entry_time_ms"))
        exit_time_ms = int(trade.get("exit_time_ms"))
    except Exception:
        log.warning("trade missing or invalid entry/exit time: %r", trade)
        return None

    # Basic info
    account_label = str(trade.get("account_label") or "main")
    side = str(trade.get("side") or "long").lower()
    strategy_id = str(trade.get("strategy_id") or "UNKNOWN")
    setup_tag = str(trade.get("setup_tag") or "UNLABELED")

    try:
        entry_price = Decimal(str(trade.get("entry_price") or "0"))
        exit_price = Decimal(str(trade.get("exit_price") or "0"))
        size = Decimal(str(trade.get("size") or "0"))
        net_pnl = Decimal(str(trade.get("net_pnl") or "0"))
    except Exception:
        entry_price = exit_price = size = net_pnl = Decimal("0")

    duration_ms = max(0, exit_time_ms - entry_time_ms)
    duration_sec = duration_ms / 1000.0

    entry_dt = _utc_from_ms(entry_time_ms)
    exit_dt = _utc_from_ms(exit_time_ms)

    # Time-of-day / session features (UTC-based, can adjust later)
    entry_hour = entry_dt.hour
    entry_minute = entry_dt.minute
    entry_dow = entry_dt.weekday()  # 0=Mon, 6=Sun

    # Very crude session buckets
    # 00-07  : Asia
    # 07-13  : London
    # 13-20  : NY
    # 20-24  : Post
    if 0 <= entry_hour < 7:
        session = "ASIA"
    elif 7 <= entry_hour < 13:
        session = "LONDON"
    elif 13 <= entry_hour < 20:
        session = "NEW_YORK"
    else:
        session = "POST"

    # Fetch candles ending at or near entry time
    candles: List[Dict[str, Any]] = []
    try:
        candles = _fetch_ohlcv(
            symbol=symbol,
            interval=interval,
            limit=lookback_candles,
            end_time_ms=entry_time_ms,
        )
    except Exception as e:
        log.warning("failed to fetch OHLCV for %s: %r", symbol, e)
        candles = []

    vol_feats = _compute_vol_features(candles)
    trend_feats = _infer_trend(candles)

    # Side sign: long=+1, short=-1
    side_sign = 1 if side == "long" else -1
    try:
        pnl_r = float(net_pnl / (entry_price * size)) if entry_price > 0 and size > 0 else 0.0
    except Exception:
        pnl_r = 0.0

    feature_row: Dict[str, Any] = {
        "schema_version": 1,
        "created_ms": _now_ms(),
        "account_label": account_label,
        "symbol": symbol,
        "side": side,
        "side_sign": side_sign,
        "strategy_id": strategy_id,
        "setup_tag": setup_tag,
        # trade outcome
        "entry_time_ms": entry_time_ms,
        "exit_time_ms": exit_time_ms,
        "duration_sec": duration_sec,
        "entry_price": float(entry_price),
        "exit_price": float(exit_price),
        "size": float(size),
        "net_pnl": float(net_pnl),
        "pnl_r": pnl_r,
        # time features
        "entry_hour": entry_hour,
        "entry_minute": entry_minute,
        "entry_dow": entry_dow,
        "session": session,
        # vol / trend features
        **vol_feats,
        **trend_feats,
        # raw extras for later use
        "raw_trade": trade,
        "interval": interval,
        "lookback_candles": lookback_candles,
    }

    return feature_row


# ---------------------------------------------------------------------------
# Bulk processor: from trades_log.jsonl -> feature_store.jsonl
# ---------------------------------------------------------------------------

def process_new_trades(
    *,
    interval: str = "1",
    lookback_candles: int = 100,
) -> None:
    """
    Process new lines from trades_log.jsonl and append feature rows to
    feature_store.jsonl. Uses a cursor file to resume from last offset.
    """
    if not TRADES_LOG_PATH.exists():
        log.info("trades_log.jsonl not found yet; nothing to process.")
        return

    pos = _load_feature_cursor()
    file_size = TRADES_LOG_PATH.stat().st_size
    if pos > file_size:
        log.info(
            "trades_log truncated (size=%s, cursor=%s). Resetting feature cursor to 0.",
            file_size,
            pos,
        )
        pos = 0

    processed = 0
    with TRADES_LOG_PATH.open("rb") as f:
        f.seek(pos)
        for raw in f:
            pos = f.tell()
            try:
                line = raw.decode("utf-8").strip()
            except Exception as e:
                log.warning("failed to decode trades_log line at pos=%s: %r", pos, e)
                continue

            if not line:
                continue

            try:
                trade = json.loads(line)
            except Exception as e:
                log.warning("invalid JSON in trades_log at pos=%s: %r", pos, e)
                continue

            feats = build_features_for_trade(
                trade,
                interval=interval,
                lookback_candles=lookback_candles,
            )
            if feats is None:
                continue

            try:
                _append_feature_row(feats)
                processed += 1
            except Exception as e:
                log.error("failed to append feature row: %r", e)

            _save_feature_cursor(pos)

    log.info("Feature builder processed %s new trades.", processed)


if __name__ == "__main__":
    # Manual run:
    #   python -m app.ai.feature_builder
    # to process any newly closed trades into feature_store.jsonl
    process_new_trades(interval="1", lookback_candles=100)
