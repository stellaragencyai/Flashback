# app/bots/trade_capture_daemon.py
# Flashback — Trade Capture Daemon v1.0
#
# Role:
# - Watch positions_bus.json over time.
# - Detect when a position for (account_label, symbol) goes from flat -> open -> flat.
# - On flat (close), emit a canonical trade record to trades_log.jsonl.
#
# v1 is intentionally simple:
# - Uses avgPrice and size from positions_bus for entry and exit snapshot.
# - Approximates PnL from (exit_price - entry_price) * size * side_sign.
# - Future versions can merge in exact fees & PnL from orders_bus / closed-pnl endpoints.

from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


logger = get_logger("trade_capture_daemon")

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

POS_SNAPSHOT_PATH: Path = STATE_DIR / "positions_bus.json"
TRADES_LOG_PATH: Path = STATE_DIR / "trades_log.jsonl"
OPEN_TRADES_STATE_PATH: Path = STATE_DIR / "open_trades_state.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class OpenTradeState:
    account_label: str
    symbol: str
    side: str           # "long" / "short"
    entry_time_ms: int
    entry_price: str    # store as string; parse with Decimal when computing
    size: str           # absolute size (contracts)


@dataclass
class ClosedTradeRecord:
    schema_version: int
    account_label: str
    symbol: str
    side: str               # "long" / "short"
    entry_time_ms: int
    exit_time_ms: int
    entry_price: str
    exit_price: str
    size: str
    gross_pnl: str          # without fees
    net_pnl: str            # same as gross for v1
    rr: str                 # risk-reward (placeholder for now)
    strategy_id: str        # placeholder, to be filled from strategy registry later
    setup_tag: str          # placeholder, to be filled manually/auto later
    meta: Dict[str, Any]    # room for extra fields


def _load_positions_bus() -> Dict[str, Any]:
    if not POS_SNAPSHOT_PATH.exists():
        return {"labels": {}, "updated_ms": 0, "schema_version": 1}
    try:
        return orjson.loads(POS_SNAPSHOT_PATH.read_bytes())
    except Exception as e:
        logger.warning("Failed to load positions_bus.json: %s", e)
        return {"labels": {}, "updated_ms": 0, "schema_version": 1}


def _load_open_trades_state() -> Dict[str, Any]:
    if not OPEN_TRADES_STATE_PATH.exists():
        return {"schema_version": 1, "open": {}}
    try:
        data = orjson.loads(OPEN_TRADES_STATE_PATH.read_bytes())
        if "open" not in data or not isinstance(data["open"], dict):
            data["open"] = {}
        return data
    except Exception as e:
        logger.warning("Failed to load open_trades_state.json: %s", e)
        return {"schema_version": 1, "open": {}}


def _save_open_trades_state(state: Dict[str, Any]) -> None:
    state["schema_version"] = 1
    if "open" not in state or not isinstance(state["open"], dict):
        state["open"] = {}
    try:
        OPEN_TRADES_STATE_PATH.write_bytes(orjson.dumps(state))
    except Exception as e:
        logger.error("Failed to save open_trades_state.json: %s", e)


def _append_closed_trade(record: ClosedTradeRecord) -> None:
    line = json.dumps(asdict(record), separators=(",", ":"))
    with TRADES_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    logger.info(
        "Recorded closed trade: %s %s %s pnl=%s",
        record.account_label,
        record.symbol,
        record.side,
        record.net_pnl,
    )


def _parse_side_and_size(row: Dict[str, Any]) -> Tuple[str, Decimal]:
    """
    From a position row, infer logical side ("long"/"short") and abs size.
    Bybit typically gives:
      - "side": "Buy" or "Sell"
      - "size": string/number
    """
    side_raw = str(row.get("side", "")).lower()
    size_raw = row.get("size") or 0
    try:
        size = Decimal(str(size_raw))
    except Exception:
        size = Decimal("0")

    if size == 0:
        return "flat", Decimal("0")

    if side_raw.startswith("buy"):
        return "long", size
    if side_raw.startswith("sell"):
        return "short", size

    # fallback
    return "long", abs(size)


def run_loop(poll_interval: float = 1.0) -> None:
    """
    Main daemon loop.
    Polls positions_bus.json every `poll_interval` seconds.
    """
    logger.info("Starting Trade Capture Daemon (poll_interval=%.2fs)", poll_interval)
    open_state = _load_open_trades_state()
    open_trades: Dict[str, Dict[str, Any]] = open_state.get("open", {})

    while True:
        try:
            snap = _load_positions_bus()
            labels = snap.get("labels", {}) or {}

            # Build current positions map: key -> row
            current_positions: Dict[str, Dict[str, Any]] = {}

            for account_label, block in labels.items():
                if not isinstance(block, dict):
                    continue
                if block.get("category") != "linear":
                    continue

                positions = block.get("positions", []) or []
                for row in positions:
                    if not isinstance(row, dict):
                        continue
                    symbol = str(row.get("symbol") or row.get("symbolName") or "").upper()
                    if not symbol:
                        continue
                    key = f"{account_label}:{symbol}"
                    current_positions[key] = row

            # Compare with open_trades (previous view)
            now_ms = _now_ms()
            new_open_state: Dict[str, Dict[str, Any]] = dict(open_trades)  # shallow copy

            # First: detect closes (was open, now missing or size=0)
            for key, ot in list(open_trades.items()):
                row = current_positions.get(key)
                if row is None:
                    # fully flat now -> closed trade
                    _handle_close(key, ot, None, now_ms)
                    new_open_state.pop(key, None)
                else:
                    side, size = _parse_side_and_size(row)
                    if size == 0 or side == "flat":
                        _handle_close(key, ot, row, now_ms)
                        new_open_state.pop(key, None)

            # Second: detect new opens (was flat, now non-zero)
            for key, row in current_positions.items():
                if key in new_open_state:
                    # already tracked as open
                    continue
                side, size = _parse_side_and_size(row)
                if size == 0 or side == "flat":
                    continue

                account_label, symbol = key.split(":", 1)
                entry_price = str(row.get("avgPrice") or row.get("avg_price") or row.get("entryPrice") or "0")
                ot = OpenTradeState(
                    account_label=account_label,
                    symbol=symbol,
                    side=side,
                    entry_time_ms=now_ms,
                    entry_price=entry_price,
                    size=str(size),
                )
                new_open_state[key] = asdict(ot)
                logger.info(
                    "Detected new open trade: %s %s %s size=%s entry_price=%s",
                    account_label,
                    symbol,
                    side,
                    ot.size,
                    ot.entry_price,
                )

            open_trades = new_open_state
            _save_open_trades_state({"schema_version": 1, "open": open_trades})

        except Exception as e:
            logger.exception("Error in Trade Capture loop: %s", e)

        time.sleep(poll_interval)


def _handle_close(
    key: str,
    ot: Dict[str, Any],
    last_row: Dict[str, Any] | None,
    exit_time_ms: int,
) -> None:
    """
    Build a ClosedTradeRecord and append it to trades_log.jsonl.
    """
    try:
        account_label, symbol = key.split(":", 1)
    except ValueError:
        logger.warning("Malformed key in open_trades_state: %s", key)
        return

    entry = OpenTradeState(**ot)

    # Exit price: from last_row.avgPrice or fallback to entry_price
    if last_row is not None:
        exit_price_raw = last_row.get("avgPrice") or last_row.get("markPrice") or entry.entry_price
    else:
        exit_price_raw = entry.entry_price

    entry_price_dec = Decimal(str(entry.entry_price))
    exit_price_dec = Decimal(str(exit_price_raw))
    size_dec = Decimal(str(entry.size))

    if entry.side == "long":
        gross_pnl_dec = (exit_price_dec - entry_price_dec) * size_dec
    else:
        gross_pnl_dec = (entry_price_dec - exit_price_dec) * size_dec

    # v1: net == gross, RR placeholder "0"
    rec = ClosedTradeRecord(
        schema_version=1,
        account_label=account_label,
        symbol=symbol,
        side=entry.side,
        entry_time_ms=entry.entry_time_ms,
        exit_time_ms=exit_time_ms,
        entry_price=str(entry_price_dec),
        exit_price=str(exit_price_dec),
        size=str(size_dec),
        gross_pnl=str(gross_pnl_dec),
        net_pnl=str(gross_pnl_dec),
        rr="0",
        strategy_id="UNKNOWN",    # TODO: fill from strategies registry
        setup_tag="UNLABELED",    # TODO: manual / auto tagging
        meta={},
    )

    _append_closed_trade(rec)


if __name__ == "__main__":
    run_loop(poll_interval=1.0)
