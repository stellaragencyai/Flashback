#!/usr/bin/env python3
# app/core/orderbook_ws.py
from __future__ import annotations

import json
import threading
import time
from typing import Dict, Any, List, Callable

import websocket  # type: ignore

BYBIT_WS_PUBLIC_URL = "wss://stream.bybit.com/v5/public/linear"

# In-memory snapshot: symbol -> {"bids": [...], "asks": [...], "ts": float}
_ORDERBOOKS: Dict[str, Dict[str, Any]] = {}
_SUBSCRIPTIONS: Dict[str, bool] = {}

def _on_message(ws, message: str) -> None:
    msg = json.loads(message)
    topic = msg.get("topic", "")
    if not topic.startswith("orderbook."):
        return
    parts = topic.split(".")
    if len(parts) < 3:
        return
    symbol = parts[-1]
    data = msg.get("data", {})
    ts = time.time()

    if "b" in data and "a" in data:
        # snapshot
        _ORDERBOOKS[symbol] = {
            "bids": data.get("b", []),
            "asks": data.get("a", []),
            "ts": ts,
        }
    elif "u" in data and ("b" in data or "a" in data):
        # delta update (simplified)
        book = _ORDERBOOKS.setdefault(symbol, {"bids": [], "asks": [], "ts": ts})
        if "b" in data:
            book["bids"] = data["b"]
        if "a" in data:
            book["asks"] = data["a"]
        book["ts"] = ts

def _on_error(ws, error) -> None:
    print(f"[ORDERBOOK_WS] error: {error}")

def _on_close(ws, *args) -> None:
    print("[ORDERBOOK_WS] closed")

def _on_open(ws) -> None:
    print("[ORDERBOOK_WS] open")
    # subscribe to all current symbols
    args: List[str] = []
    for sym in list(_SUBSCRIPTIONS.keys()):
        args.append(f"orderbook.1.{sym}")
    if args:
        sub = {"op": "subscribe", "args": args}
        ws.send(json.dumps(sub))

def _run_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(
                BYBIT_WS_PUBLIC_URL,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever()
        except Exception as e:
            print(f"[ORDERBOOK_WS] reconnect after error: {e}")
            time.sleep(3)

# Start background thread once
_thread_started = False

def start_orderbook_ws():
    global _thread_started
    if _thread_started:
        return
    _thread_started = True
    th = threading.Thread(target=_run_ws, daemon=True)
    th.start()

def subscribe_symbol(symbol: str) -> None:
    sym = symbol.upper()
    _SUBSCRIPTIONS[sym] = True

def get_orderbook_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Return last snapshot: {"bids": [[px,qty],...], "asks": [[px,qty],...], "ts": float}
    """
    return _ORDERBOOKS.get(symbol.upper(), {})
