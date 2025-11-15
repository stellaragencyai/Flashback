#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 / Flashback — AI Event Store (SQLite)

Purpose:
- Central write-path for all AI / analytics events:
  • Signals
  • Orders
  • Executions
  • Position snapshots
  • Trade summaries
  • Regime snapshots

- Backed by a dedicated SQLite db:
    state/ai_store.db

Usage:
- Import and call the log_* helpers from your bots / core:
    from app.core import ai_store
    ai_store.log_signal({...})
    ai_store.log_order({...})
    ...

Schema:
- Schema is aligned to app.core.ai_schema.* TypedDicts.
- Arrays / dicts are stored as JSON strings.
"""

from __future__ import annotations

import sqlite3
import json
import time
from pathlib import Path
from typing import Any, Dict

from app.core.ai_schema import (
    SignalLog,
    OrderLog,
    ExecutionLog,
    PositionSnapshot,
    TradeSummaryLog,
    RegimeSnapshotLog,
)

# ---------- Paths & DB init ----------

# This file is expected at: project_root/app/core/ai_store.py
THIS_FILE = Path(__file__).resolve()
CORE_DIR = THIS_FILE.parent          # .../app/core
APP_DIR = CORE_DIR.parent            # .../app
ROOT_DIR = APP_DIR.parent            # project_root

STATE_DIR = ROOT_DIR / "state"
STATE_DIR.mkdir(exist_ok=True, parents=True)

DB_PATH = STATE_DIR / "ai_store.db"


def _get_conn() -> sqlite3.Connection:
    """
    Get a SQLite connection with sane defaults.
    Use a short timeout to avoid lock issues from multiple bots.
    """
    conn = sqlite3.connect(DB_PATH, timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    """
    Create tables if they don't exist.
    Schema is intentionally simple and denormalized:
    - JSON blobs for lists/dicts
    """
    conn = _get_conn()
    cur = conn.cursor()

    # Signals
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id       TEXT UNIQUE,
            ts_ms           INTEGER,
            symbol          TEXT,
            timeframe       TEXT,
            side            TEXT,
            source          TEXT,
            confidence      REAL,
            stop_hint_price REAL,
            owner           TEXT,
            sub_uid         TEXT,
            strategy_role   TEXT,
            regime_tags     TEXT,   -- JSON list
            extra           TEXT    -- JSON dict
        )
        """
    )

    # Orders
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_orders (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id      TEXT UNIQUE,
            ts_ms         INTEGER,
            symbol        TEXT,
            side          TEXT,
            order_type    TEXT,
            qty           REAL,
            price         REAL,
            signal_id     TEXT,
            sub_uid       TEXT,
            owner         TEXT,
            strategy_role TEXT,
            exit_profile  TEXT,
            reduce_only   INTEGER,
            post_only     INTEGER,
            extra         TEXT      -- JSON dict
        )
        """
    )

    # Executions
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_executions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            exec_id       TEXT UNIQUE,
            order_id      TEXT,
            ts_ms         INTEGER,
            symbol        TEXT,
            side          TEXT,
            qty           REAL,
            price         REAL,
            fee           REAL,
            pnl_increment REAL,
            sub_uid       TEXT,
            owner         TEXT,
            strategy_role TEXT,
            latency_ms    INTEGER,
            extra         TEXT      -- JSON dict
        )
        """
    )

    # Position snapshots
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_positions (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms                     INTEGER,
            sub_uid                   TEXT,
            symbol                    TEXT,
            size                      REAL,
            avg_entry                 REAL,
            unrealized_pnl            REAL,
            max_favorable_excursion   REAL,
            max_adverse_excursion     REAL,
            owner                     TEXT,
            strategy_role             TEXT,
            extra                     TEXT    -- JSON dict
        )
        """
    )

    # Trade summaries
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_trades (
            id                         INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id                   TEXT UNIQUE,
            sub_uid                    TEXT,
            symbol                     TEXT,
            side                       TEXT,
            opened_ts_ms               INTEGER,
            closed_ts_ms               INTEGER,
            outcome                    TEXT,
            r_multiple                 REAL,
            realized_pnl               REAL,
            signal_id                  TEXT,
            owner                      TEXT,
            strategy_role              TEXT,
            entry_price                REAL,
            exit_price                 REAL,
            max_favorable_excursion_r  REAL,
            max_adverse_excursion_r    REAL,
            holding_ms                 INTEGER,
            exit_reason                TEXT,
            regime_at_entry            TEXT,   -- JSON or short string
            regime_at_exit             TEXT,
            extra                      TEXT    -- JSON dict
        )
        """
    )

    # Regime snapshots
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_regimes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms       INTEGER,
            symbol      TEXT,
            timeframe   TEXT,
            adx         REAL,
            atr_pct     REAL,
            vol_zscore  REAL,
            trend_state TEXT,
            session     TEXT,
            tags        TEXT,   -- JSON list
            extra       TEXT    -- JSON dict
        )
        """
    )

    conn.commit()
    conn.close()


# Initialize on import
_init_db()


# ---------- Helpers ----------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_dumps(obj: Any) -> str:
    if obj is None:
        return "null"
    try:
        return json.dumps(obj, separators=(",", ":"))
    except Exception:
        # Last resort
        return "null"


def _bool_to_int(value: Any) -> int:
    if value is True:
        return 1
    if value is False:
        return 0
    return 0


# ---------- Public logging API ----------

def log_signal(data: SignalLog) -> None:
    """
    Insert or replace a signal row into ai_signals.
    Missing ts_ms is filled with "now".
    """
    d: Dict[str, Any] = dict(data)
    if "ts_ms" not in d or d["ts_ms"] is None:
        d["ts_ms"] = _now_ms()

    regime_tags = d.get("regime_tags") or []
    extra = d.get("extra") or {}

    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO ai_signals (
            signal_id, ts_ms, symbol, timeframe, side, source,
            confidence, stop_hint_price, owner, sub_uid,
            strategy_role, regime_tags, extra
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            d.get("signal_id"),
            d.get("ts_ms"),
            d.get("symbol"),
            d.get("timeframe"),
            d.get("side"),
            d.get("source"),
            d.get("confidence"),
            d.get("stop_hint_price"),
            d.get("owner"),
            d.get("sub_uid"),
            d.get("strategy_role"),
            _json_dumps(regime_tags),
            _json_dumps(extra),
        ),
    )
    conn.commit()
    conn.close()


def log_order(data: OrderLog) -> None:
    """
    Insert or replace an order row into ai_orders.
    Missing ts_ms is filled with "now".
    """
    d: Dict[str, Any] = dict(data)
    if "ts_ms" not in d or d["ts_ms"] is None:
        d["ts_ms"] = _now_ms()

    extra = d.get("extra") or {}

    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO ai_orders (
            order_id, ts_ms, symbol, side, order_type,
            qty, price, signal_id, sub_uid, owner, strategy_role,
            exit_profile, reduce_only, post_only, extra
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            d.get("order_id"),
            d.get("ts_ms"),
            d.get("symbol"),
            d.get("side"),
            d.get("order_type"),
            d.get("qty"),
            d.get("price"),
            d.get("signal_id"),
            d.get("sub_uid"),
            d.get("owner"),
            d.get("strategy_role"),
            d.get("exit_profile"),
            _bool_to_int(d.get("reduce_only")),
            _bool_to_int(d.get("post_only")),
            _json_dumps(extra),
        ),
    )
    conn.commit()
    conn.close()


def log_execution(data: ExecutionLog) -> None:
    """
    Insert or replace an execution row into ai_executions.
    """
    d: Dict[str, Any] = dict(data)
    if "ts_ms" not in d or d["ts_ms"] is None:
        d["ts_ms"] = _now_ms()

    extra = d.get("extra") or {}

    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO ai_executions (
            exec_id, order_id, ts_ms, symbol, side,
            qty, price, fee, pnl_increment,
            sub_uid, owner, strategy_role, latency_ms, extra
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            d.get("exec_id"),
            d.get("order_id"),
            d.get("ts_ms"),
            d.get("symbol"),
            d.get("side"),
            d.get("qty"),
            d.get("price"),
            d.get("fee"),
            d.get("pnl_increment"),
            d.get("sub_uid"),
            d.get("owner"),
            d.get("strategy_role"),
            d.get("latency_ms"),
            _json_dumps(extra),
        ),
    )
    conn.commit()
    conn.close()


def log_position_snapshot(data: PositionSnapshot) -> None:
    """
    Insert a position snapshot into ai_positions.
    This is append-only; each call records a new row.
    """
    d: Dict[str, Any] = dict(data)
    if "ts_ms" not in d or d["ts_ms"] is None:
        d["ts_ms"] = _now_ms()

    extra = d.get("extra") or {}

    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ai_positions (
            ts_ms, sub_uid, symbol, size, avg_entry,
            unrealized_pnl, max_favorable_excursion,
            max_adverse_excursion, owner, strategy_role, extra
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            d.get("ts_ms"),
            d.get("sub_uid"),
            d.get("symbol"),
            d.get("size"),
            d.get("avg_entry"),
            d.get("unrealized_pnl"),
            d.get("max_favorable_excursion"),
            d.get("max_adverse_excursion"),
            d.get("owner"),
            d.get("strategy_role"),
            _json_dumps(extra),
        ),
    )
    conn.commit()
    conn.close()


def log_trade_summary(data: TradeSummaryLog) -> None:
    """
    Insert or replace a trade summary row into ai_trades.
    trade_id is treated as unique key.
    """
    d: Dict[str, Any] = dict(data)

    extra = d.get("extra") or {}

    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO ai_trades (
            trade_id, sub_uid, symbol, side,
            opened_ts_ms, closed_ts_ms,
            outcome, r_multiple, realized_pnl,
            signal_id, owner, strategy_role,
            entry_price, exit_price,
            max_favorable_excursion_r, max_adverse_excursion_r,
            holding_ms, exit_reason,
            regime_at_entry, regime_at_exit, extra
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            d.get("trade_id"),
            d.get("sub_uid"),
            d.get("symbol"),
            d.get("side"),
            d.get("opened_ts_ms"),
            d.get("closed_ts_ms"),
            d.get("outcome"),
            d.get("r_multiple"),
            d.get("realized_pnl"),
            d.get("signal_id"),
            d.get("owner"),
            d.get("strategy_role"),
            d.get("entry_price"),
            d.get("exit_price"),
            d.get("max_favorable_excursion_r"),
            d.get("max_adverse_excursion_r"),
            d.get("holding_ms"),
            d.get("exit_reason"),
            d.get("regime_at_entry"),
            d.get("regime_at_exit"),
            _json_dumps(extra),
        ),
    )
    conn.commit()
    conn.close()


def log_regime_snapshot(data: RegimeSnapshotLog) -> None:
    """
    Insert a regime snapshot into ai_regimes.
    This is append-only.
    """
    d: Dict[str, Any] = dict(data)
    if "ts_ms" not in d or d["ts_ms"] is None:
        d["ts_ms"] = _now_ms()

    tags = d.get("tags") or []
    extra = d.get("extra") or {}

    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ai_regimes (
            ts_ms, symbol, timeframe,
            adx, atr_pct, vol_zscore,
            trend_state, session, tags, extra
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            d.get("ts_ms"),
            d.get("symbol"),
            d.get("timeframe"),
            d.get("adx"),
            d.get("atr_pct"),
            d.get("vol_zscore"),
            d.get("trend_state"),
            d.get("session"),
            _json_dumps(tags),
            _json_dumps(extra),
        ),
    )
    conn.commit()
    conn.close()
