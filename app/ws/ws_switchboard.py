#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard v2.3 (WS version)

Purpose
-------
Single source of truth for live WebSocket feeds.

v1 scope:
- Connect to Bybit v5 PRIVATE WS for ONE unified account.
- Authenticate and subscribe to "position".
- On each snapshot/delta:
    • Take RAW Bybit position rows (category='linear', size>0)
    • Write to state/positions_bus.json in the format expected by
      app.core.position_bus:

      {
        "version": 1,
        "updated_ms": 1763752000123,
        "labels": {
          "<ACCOUNT_LABEL>": {
            "category": "linear",
            "positions": [
              { ...raw Bybit position row... },
              ...
            ]
          }
        }
      }

    Where <ACCOUNT_LABEL> is configurable via env:
        ACCOUNT_LABEL=main        (default)
        ACCOUNT_LABEL=flashback10 (for that subaccount), etc.

v2 scope:
- Add PUBLIC WS client for HFT / microstructure needs.
- Subscribe to:
    • orderbook.50.<SYMBOL>  (L2 book snapshots/deltas)
    • publicTrade.<SYMBOL>   (recent trades stream)
- Write to:
    • state/orderbook_bus.json   (per-symbol L2 snapshots)
    • state/trades_bus.json      (rolling recent trades per symbol)

v2.3 enhancements:
- Add heartbeat file per account:
    • state/ws_switchboard_heartbeat_<ACCOUNT_LABEL>.txt
    • Interval controlled by WS_HEARTBEAT_SECONDS (default: 20s)
- Allow PUBLIC WS to be disabled by empty WS_PUBLIC_SYMBOLS.
- Default PUBLIC URL tuned for v5 linear: wss://stream.bybit.com/v5/public/linear
"""

from __future__ import annotations

import hmac
import hashlib
import os
import time
import threading
from pathlib import Path
from typing import Any, Dict, List

import orjson

# ---------------------------------------------------------------------------
# Logger (robust import)
# ---------------------------------------------------------------------------

try:
    import importlib

    _log_module = importlib.import_module("app.core.log")
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


logger = get_logger("ws_switchboard")

try:
    import websocket  # type: ignore
except ImportError:
    websocket = None  # we will complain loudly in main()

# ---------------------------------------------------------------------------
# Env / account label
# ---------------------------------------------------------------------------

ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"
logger.info("WS Switchboard ACCOUNT_LABEL set to: %s", ACCOUNT_LABEL)

HEARTBEAT_SECONDS: int = int(os.getenv("WS_HEARTBEAT_SECONDS", "20"))

# ---------------------------------------------------------------------------
# Paths and snapshot helpers
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        ROOT: Path = Path(__file__).resolve().parents[2]

    settings = _DummySettings()  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

POS_SNAPSHOT_PATH: Path = STATE_DIR / "positions_bus.json"
ORDERBOOK_SNAPSHOT_PATH: Path = STATE_DIR / "orderbook_bus.json"
TRADES_SNAPSHOT_PATH: Path = STATE_DIR / "trades_bus.json"

HEARTBEAT_PATH: Path = STATE_DIR / f"ws_switchboard_heartbeat_{ACCOUNT_LABEL}.txt"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _write_heartbeat() -> None:
    try:
        HEARTBEAT_PATH.write_text(str(_now_ms()), encoding="utf-8")
    except Exception as e:
        logger.error("Failed to write heartbeat file %s: %s", HEARTBEAT_PATH, e)


def _heartbeat_loop(interval: int) -> None:
    logger.info(
        "Starting WS heartbeat loop (interval=%ss, file=%s)",
        interval,
        HEARTBEAT_PATH,
    )
    while True:
        _write_heartbeat()
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Positions bus helpers (PRIVATE WS, single account per process)
# ---------------------------------------------------------------------------

def _load_positions_snapshot() -> Dict[str, Any]:
    if not POS_SNAPSHOT_PATH.exists():
        return {
            "version": 1,
            "updated_ms": 0,
            "labels": {},
        }
    try:
        data = orjson.loads(POS_SNAPSHOT_PATH.read_bytes())
        if not isinstance(data, dict):
            raise ValueError("positions_bus root is not dict")
        if "labels" not in data or not isinstance(data["labels"], dict):
            data["labels"] = {}
        if "version" not in data:
            data["version"] = 1
        if "updated_ms" not in data:
            data["updated_ms"] = 0
        return data
    except Exception as e:
        logger.warning("Failed to load positions_bus.json (starting fresh): %s", e)
        return {
            "version": 1,
            "updated_ms": 0,
            "labels": {},
        }


def _save_positions_snapshot(snapshot: Dict[str, Any]) -> None:
    snapshot["version"] = 1
    snapshot["updated_ms"] = _now_ms()
    if "labels" not in snapshot or not isinstance(snapshot["labels"], dict):
        snapshot["labels"] = {}
    try:
        POS_SNAPSHOT_PATH.write_bytes(orjson.dumps(snapshot))
    except Exception as e:
        logger.error("Failed to save positions_bus.json: %s", e)


def _update_label_from_ws_rows(
    account_label: str, data_rows: List[Dict[str, Any]]
) -> None:
    snapshot = _load_positions_snapshot()
    labels = snapshot.get("labels", {})

    filtered: List[Dict[str, Any]] = []
    for row in data_rows:
        if not isinstance(row, dict):
            continue

        category = str(row.get("category", "")).lower()
        if category != "linear":
            continue

        try:
            size = float(row.get("size") or 0)
        except Exception:
            size = 0.0

        if size == 0:
            continue

        filtered.append(row)

    labels[account_label] = {
        "category": "linear",
        "positions": filtered,
    }
    snapshot["labels"] = labels
    _save_positions_snapshot(snapshot)

    logger.debug(
        "WS Switchboard updated '%s' positions: %d open linear positions",
        account_label,
        len(filtered),
    )


# ---------------------------------------------------------------------------
# Orderbook bus helpers (PUBLIC WS)
# ---------------------------------------------------------------------------

def _load_orderbook_snapshot() -> Dict[str, Any]:
    if not ORDERBOOK_SNAPSHOT_PATH.exists():
        return {
            "version": 1,
            "updated_ms": 0,
            "symbols": {},
        }
    try:
        data = orjson.loads(ORDERBOOK_SNAPSHOT_PATH.read_bytes())
        if not isinstance(data, dict):
            raise ValueError("orderbook_bus root is not dict")
        if "symbols" not in data or not isinstance(data["symbols"], dict):
            data["symbols"] = {}
        if "version" not in data:
            data["version"] = 1
        if "updated_ms" not in data:
            data["updated_ms"] = 0
        return data
    except Exception as e:
        logger.warning("Failed to load orderbook_bus.json (starting fresh): %s", e)
        return {
            "version": 1,
            "updated_ms": 0,
            "symbols": {},
        }


def _save_orderbook_snapshot(snapshot: Dict[str, Any]) -> None:
    snapshot["version"] = 1
    snapshot["updated_ms"] = _now_ms()
    if "symbols" not in snapshot or not isinstance(snapshot["symbols"], dict):
        snapshot["symbols"] = {}
    try:
        ORDERBOOK_SNAPSHOT_PATH.write_bytes(orjson.dumps(snapshot))
    except Exception as e:
        logger.error("Failed to save orderbook_bus.json: %s", e)


def _update_orderbook_from_ws(topic: str, data: Any) -> None:
    if not topic.startswith("orderbook."):
        return

    parts = topic.split(".")
    symbol = parts[-1] if len(parts) >= 3 else None
    if not symbol:
        return

    rows: List[Dict[str, Any]] = []
    if isinstance(data, list):
        rows = [d for d in data if isinstance(d, dict)]
    elif isinstance(data, dict):
        rows = [data]
    else:
        return

    if not rows:
        return

    latest = rows[-1]
    bids = latest.get("b") or latest.get("bid") or []
    asks = latest.get("a") or latest.get("ask") or []
    ts_ms = int(latest.get("ts", _now_ms()))

    snapshot = _load_orderbook_snapshot()
    symbols = snapshot.get("symbols", {})

    symbols[symbol] = {
        "bids": bids,
        "asks": asks,
        "ts_ms": ts_ms,
    }
    snapshot["symbols"] = symbols
    _save_orderbook_snapshot(snapshot)

    logger.debug("Updated orderbook for %s (bids=%d, asks=%d)", symbol, len(bids), len(asks))


# ---------------------------------------------------------------------------
# Trades bus helpers (PUBLIC WS)
# ---------------------------------------------------------------------------

_MAX_TRADES_PER_SYMBOL = 200


def _load_trades_snapshot() -> Dict[str, Any]:
    if not TRADES_SNAPSHOT_PATH.exists():
        return {
            "version": 1,
            "updated_ms": 0,
            "symbols": {},
        }
    try:
        data = orjson.loads(TRADES_SNAPSHOT_PATH.read_bytes())
        if not isinstance(data, dict):
            raise ValueError("trades_bus root is not dict")
        if "symbols" not in data or not isinstance(data["symbols"], dict):
            data["symbols"] = {}
        if "version" not in data:
            data["version"] = 1
        if "updated_ms" not in data:
            data["updated_ms"] = 0
        return data
    except Exception as e:
        logger.warning("Failed to load trades_bus.json (starting fresh): %s", e)
        return {
            "version": 1,
            "updated_ms": 0,
            "symbols": {},
        }


def _save_trades_snapshot(snapshot: Dict[str, Any]) -> None:
    snapshot["version"] = 1
    snapshot["updated_ms"] = _now_ms()
    if "symbols" not in snapshot or not isinstance(snapshot["symbols"], dict):
        snapshot["symbols"] = {}
    try:
        TRADES_SNAPSHOT_PATH.write_bytes(orjson.dumps(snapshot))
    except Exception as e:
        logger.error("Failed to save trades_bus.json: %s", e)


def _append_trades_from_ws(topic: str, data: Any) -> None:
    if not topic.startswith("publicTrade."):
        return

    parts = topic.split(".")
    symbol = parts[-1] if len(parts) >= 2 else None
    if not symbol:
        return

    if isinstance(data, dict):
        rows = [data]
    elif isinstance(data, list):
        rows = [d for d in data if isinstance(d, dict)]
    else:
        return

    if not rows:
        return

    snapshot = _load_trades_snapshot()
    symbols = snapshot.get("symbols", {})

    sym_block = symbols.get(symbol, {})
    existing = sym_block.get("trades", [])
    if not isinstance(existing, list):
        existing = []

    existing.extend(rows)
    if len(existing) > _MAX_TRADES_PER_SYMBOL:
        existing = existing[-_MAX_TRADES_PER_SYMBOL:]

    sym_block["trades"] = existing
    symbols[symbol] = sym_block
    snapshot["symbols"] = symbols
    _save_trades_snapshot(snapshot)

    logger.debug("Appended %d trades for %s (total=%d)", len(rows), symbol, len(existing))


# ---------------------------------------------------------------------------
# PRIVATE WS auth helper (v5)
# ---------------------------------------------------------------------------

def _build_ws_auth_payload(api_key: str, api_secret: str) -> Dict[str, Any]:
    """
    Build Bybit v5 WebSocket auth payload:

        expires = now_ms + 10_000
        sign_str = "GET/realtime" + str(expires)
        signature = HMAC_SHA256(secret, sign_str)

        {"op": "auth", "args": [api_key, expires, signature]}
    """
    expires = _now_ms() + 10_000
    sign_str = f"GET/realtime{expires}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        sign_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return {
        "op": "auth",
        "args": [api_key, expires, signature],
    }


# ---------------------------------------------------------------------------
# PRIVATE WS callbacks (positions)
# ---------------------------------------------------------------------------

def _on_open_private(ws, api_key: str, api_secret: str) -> None:
    logger.info("PRIVATE WS connection opened, authenticating with Bybit...")

    try:
        auth_payload = _build_ws_auth_payload(api_key, api_secret)
        ws.send(orjson.dumps(auth_payload).decode("utf-8"))
        logger.info("PRIVATE auth payload sent.")
    except Exception as e:
        logger.error("Failed to send PRIVATE auth payload: %s", e)
        return

    sub_payload = {
        "op": "subscribe",
        "args": ["position"],
    }

    try:
        ws.send(orjson.dumps(sub_payload).decode("utf-8"))
        logger.info("PRIVATE subscribe payload for 'position' sent.")
    except Exception as e:
        logger.error("Failed to send PRIVATE subscribe payload: %s", e)


def _on_message_private(ws, message: str) -> None:
    try:
        msg = orjson.loads(message)
    except Exception:
        logger.debug("PRIVATE non-JSON WS message: %s", message)
        return

    topic = msg.get("topic")

    if topic == "position":
        data = msg.get("data") or []
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            data = []
        _update_label_from_ws_rows(ACCOUNT_LABEL, data)
        return

    if msg.get("op") == "auth" and msg.get("success") is True:
        logger.info("Bybit PRIVATE WS auth successful.")
        return

    if msg.get("op") == "subscribe" and msg.get("success") is True:
        logger.info("PRIVATE subscribe success: %s", msg.get("args"))
        return

    logger.debug("PRIVATE WS message: %s", msg)


def _on_error_private(ws, error: Any) -> None:
    logger.error("PRIVATE WS error: %s", error)


def _on_close_private(ws, status_code: Any, msg: Any) -> None:
    logger.warning("PRIVATE WS closed: code=%s, msg=%s", status_code, msg)


def _run_private_ws(api_key: str, api_secret: str, ws_url: str) -> None:
    websocket.enableTrace(False)

    def _on_open_cb(ws_):
        _on_open_private(ws_, api_key, api_secret)

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=_on_open_cb,
        on_message=_on_message_private,
        on_error=_on_error_private,
        on_close=_on_close_private,
    )

    while True:
        try:
            logger.info("Starting PRIVATE WS loop...")
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
            logger.warning("PRIVATE WS connection ended, retrying in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("PRIVATE WS Switchboard interrupted by user, exiting PRIVATE loop.")
            break
        except Exception as e:
            logger.exception("Unexpected error in PRIVATE WS loop: %s", e)
            time.sleep(5)


# ---------------------------------------------------------------------------
# PUBLIC WS callbacks (orderbook + trades)
# ---------------------------------------------------------------------------

def _on_open_public(ws, symbols: List[str]) -> None:
    args: List[str] = []
    for sym in symbols:
        sym = sym.strip().upper()
        if not sym:
            continue
        args.append(f"orderbook.50.{sym}")
        args.append(f"publicTrade.{sym}")

    if not args:
        logger.warning("PUBLIC WS opened but no symbols to subscribe to.")
        return

    sub_payload = {
        "op": "subscribe",
        "args": args,
    }

    try:
        ws.send(orjson.dumps(sub_payload).decode("utf-8"))
        logger.info("PUBLIC subscribe payload sent for topics: %s", args)
    except Exception as e:
        logger.error("Failed to send PUBLIC subscribe payload: %s", e)


def _on_message_public(ws, message: str) -> None:
    try:
        msg = orjson.loads(message)
    except Exception:
        logger.debug("PUBLIC non-JSON WS message: %s", message)
        return

    topic = msg.get("topic")
    if not topic:
        if msg.get("op") == "subscribe" and msg.get("success") is True:
            logger.info("PUBLIC subscribe success: %s", msg.get("args"))
        else:
            logger.debug("PUBLIC WS message (no topic): %s", msg)
        return

    data = msg.get("data")
    if topic.startswith("orderbook."):
        _update_orderbook_from_ws(topic, data)
        return

    if topic.startswith("publicTrade."):
        _append_trades_from_ws(topic, data)
        return

    logger.debug("PUBLIC WS message (unhandled topic %s): %s", topic, msg)


def _on_error_public(ws, error: Any) -> None:
    logger.error("PUBLIC WS error: %s", error)


def _on_close_public(ws, status_code: Any, msg: Any) -> None:
    logger.warning("PUBLIC WS closed: code=%s, msg=%s", status_code, msg)


def _run_public_ws(symbols: List[str], ws_url: str) -> None:
    websocket.enableTrace(False)

    clean_symbols = [s.strip().upper() for s in symbols if s.strip()]
    logger.info("PUBLIC WS will subscribe to symbols: %s", clean_symbols)

    def _on_open_cb(ws_):
        _on_open_public(ws_, clean_symbols)

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=_on_open_cb,
        on_message=_on_message_public,
        on_error=_on_error_public,
        on_close=_on_close_public,
    )

    while True:
        try:
            logger.info("Starting PUBLIC WS loop...")
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
            logger.warning("PUBLIC WS connection ended, retrying in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("PUBLIC WS Switchboard interrupted by user, exiting PUBLIC loop.")
            break
        except Exception as e:
            logger.exception("Unexpected error in PUBLIC WS loop: %s", e)
            time.sleep(5)


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def main() -> None:
    if websocket is None:
        logger.error("websocket-client is not installed. Run: pip install websocket-client")
        return

    api_key = os.getenv("BYBIT_MAIN_API_KEY") or os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_MAIN_API_SECRET") or os.getenv("BYBIT_API_SECRET")

    if not api_key or not api_secret:
        logger.error(
            "Missing Bybit API keys for PRIVATE WS. "
            "Set BYBIT_MAIN_API_KEY / BYBIT_MAIN_API_SECRET "
            "or BYBIT_API_KEY / BYBIT_API_SECRET."
        )
        return

    ws_private_url = os.getenv("BYBIT_WS_PRIVATE_URL", "wss://stream.bybit.com/v5/private")
    ws_public_url = os.getenv(
        "BYBIT_WS_PUBLIC_URL",
        "wss://stream.bybit.com/v5/public/linear",
    )

    raw_public_symbols = os.getenv("WS_PUBLIC_SYMBOLS", "BTCUSDT,ETHUSDT").strip()
    if raw_public_symbols:
        public_symbols = [s.strip() for s in raw_public_symbols.split(",") if s.strip()]
    else:
        public_symbols = []

    logger.info("Starting WS Switchboard v2.3")
    logger.info("ACCOUNT_LABEL       : %s", ACCOUNT_LABEL)
    logger.info("PRIVATE WS endpoint : %s", ws_private_url)
    logger.info("PUBLIC  WS endpoint : %s", ws_public_url)
    logger.info("PUBLIC  WS symbols  : %s", public_symbols if public_symbols else "[DISABLED]")
    logger.info("HEARTBEAT file      : %s", HEARTBEAT_PATH)
    logger.info("HEARTBEAT interval  : %ss", HEARTBEAT_SECONDS)

    _write_heartbeat()
    hb_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(HEARTBEAT_SECONDS,),
        name="ws_heartbeat_thread",
        daemon=True,
    )
    hb_thread.start()

    private_thread = threading.Thread(
        target=_run_private_ws,
        args=(api_key, api_secret, ws_private_url),
        name="ws_private_thread",
        daemon=True,
    )
    private_thread.start()

    if public_symbols:
        public_thread = threading.Thread(
            target=_run_public_ws,
            args=(public_symbols, ws_public_url),
            name="ws_public_thread",
            daemon=True,
        )
        public_thread.start()
    else:
        logger.info("Skipping PUBLIC WS client (no WS_PUBLIC_SYMBOLS configured).")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("WS Switchboard main interrupted by user, exiting.")


if __name__ == "__main__":
    main()
