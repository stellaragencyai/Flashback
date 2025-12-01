# app/core/orders_bus.py
# Flashback — Orders Bus v1.0
#
# Single source of truth for normalized order events.
#
# This module writes to:
#   state/orders_bus.json
#
# Schema (events is a list of normalized order events):
# {
#   "schema_version": 1,
#   "updated_ms": 1763752000999,
#   "events": [
#       {
#         "ts_ms": 1763751999000,
#         "account_label": "main",
#         "symbol": "BTCUSDT",
#         "order_id": "string",
#         "client_order_id": "MAIN_12345",
#         "side": "Buy",
#         "position_side": "Long",
#         "order_type": "Limit",
#         "status": "New",
#         "event_type": "NEW",
#         "price": "65000.0",
#         "qty": "0.010",
#         "cum_exec_qty": "0.005",
#         "cum_exec_value": "325.0",
#         "cum_exec_fee": "0.02",
#         "reduce_only": false,
#         "raw": { ... }
#       }
#   ]
# }

from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson

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


logger = get_logger("orders_bus")

try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = getattr(settings, "ROOT")
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

ORDERS_BUS_PATH: Path = STATE_DIR / "orders_bus.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_orders_bus() -> Dict[str, Any]:
    if not ORDERS_BUS_PATH.exists():
        return {
            "schema_version": 1,
            "updated_ms": 0,
            "events": [],
        }
    try:
        data = orjson.loads(ORDERS_BUS_PATH.read_bytes())
        if not isinstance(data, dict):
            raise ValueError("orders_bus root is not dict")
        if "events" not in data or not isinstance(data["events"], list):
            data["events"] = []
        if "schema_version" not in data:
            data["schema_version"] = 1
        if "updated_ms" not in data:
            data["updated_ms"] = 0
        return data
    except Exception as e:
        logger.warning("Failed to load orders_bus.json (starting fresh): %s", e)
        return {
            "schema_version": 1,
            "updated_ms": 0,
            "events": [],
        }


def _save_orders_bus(snapshot: Dict[str, Any]) -> None:
    snapshot["schema_version"] = 1
    snapshot["updated_ms"] = _now_ms()
    if "events" not in snapshot or not isinstance(snapshot["events"], list):
        snapshot["events"] = []
    try:
        ORDERS_BUS_PATH.write_bytes(orjson.dumps(snapshot))
    except Exception as e:
        logger.error("Failed to save orders_bus.json: %s", e)


def record_order_event(
    *,
    account_label: str,
    symbol: str,
    order_id: str,
    side: str,
    order_type: str,
    status: str,
    event_type: str,
    price: Optional[str] = None,
    qty: Optional[str] = None,
    cum_exec_qty: Optional[str] = None,
    cum_exec_value: Optional[str] = None,
    cum_exec_fee: Optional[str] = None,
    position_side: Optional[str] = None,
    reduce_only: Optional[bool] = None,
    client_order_id: Optional[str] = None,
    ts_ms: Optional[int] = None,
    raw: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Normalize and append a single order event to orders_bus.json.

    event_type:
        - "NEW"     : order placed
        - "UPDATE"  : partial fill / status change
        - "FILL"    : fully filled
        - "CANCEL"  : cancelled
    """
    snapshot = _load_orders_bus()
    events: List[Dict[str, Any]] = snapshot.get("events", [])

    ts = ts_ms if ts_ms is not None else _now_ms()

    event: Dict[str, Any] = {
        "ts_ms": ts,
        "account_label": account_label,
        "symbol": symbol,
        "order_id": order_id,
        "client_order_id": client_order_id,
        "side": side,
        "position_side": position_side,
        "order_type": order_type,
        "status": status,
        "event_type": event_type,
        "price": price,
        "qty": qty,
        "cum_exec_qty": cum_exec_qty,
        "cum_exec_value": cum_exec_value,
        "cum_exec_fee": cum_exec_fee,
        "reduce_only": bool(reduce_only) if reduce_only is not None else None,
        "raw": raw or {},
    }

    events.append(event)

    # Optional: trim to last N events to prevent unbounded growth.
    max_events = 5000
    if len(events) > max_events:
        events = events[-max_events:]

    snapshot["events"] = events
    _save_orders_bus(snapshot)

    logger.debug(
        "Recorded order event: %s %s %s %s",
        account_label,
        symbol,
        order_id,
        event_type,
    )
