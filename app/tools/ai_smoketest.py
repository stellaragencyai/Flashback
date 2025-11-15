#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 / Flashback — AI Store Smoke Test

Purpose:
- Verify that:
    • app.core.ai_schema imports correctly
    • app.core.ai_store initializes SQLite at state/ai_store.db
    • app.core.ai_hooks can log:
        - a signal
        - an order
        - a trade summary
- Print simple row counts so you know the AI pipeline is alive.

Usage (from project root):
    python -m app.tools.ai_smoketest
or:
    python app/tools/ai_smoketest.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
import time

# Import the AI components
from app.core import ai_store  # noqa: F401  # ensures DB init
from app.core.ai_hooks import (
    log_signal_from_engine,
    log_order_basic,
    log_trade_summary_basic,
)


# ---------- Paths ----------

THIS_FILE = Path(__file__).resolve()
TOOLS_DIR = THIS_FILE.parent        # .../app/tools
APP_DIR = TOOLS_DIR.parent          # .../app
ROOT_DIR = APP_DIR.parent           # project_root
STATE_DIR = ROOT_DIR / "state"
DB_PATH = STATE_DIR / "ai_store.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def main() -> None:
    print("=== AI Store Smoke Test ===")
    print(f"Project root: {ROOT_DIR}")
    print(f"AI DB path:   {DB_PATH}")

    if not DB_PATH.exists():
        print("AI DB does not exist yet. It should be created on import of app.core.ai_store.")
    else:
        print("AI DB already exists, good.")

    # 1) Log a fake signal
    print("\n[1/3] Logging dummy signal...")
    signal_id = log_signal_from_engine(
        symbol="BTCUSDT",
        timeframe="5m",
        side="LONG",
        source="ai_smoketest",
        confidence=0.9,
        stop_hint=42000.0,
        owner="MAIN_TEACHER",
        sub_uid="TEST_SUBUID",
        strategy_role="SMOKETEST_ROLE",
        regime_tags=["smoketest", "unit"],
        extra={"note": "dummy signal from ai_smoketest"},
    )
    print(f"  -> signal_id: {signal_id}")

    # 2) Log a fake order associated with that signal
    print("\n[2/3] Logging dummy order...")
    log_order_basic(
        order_id=f"TEST_ORDER_{int(time.time())}",
        symbol="BTCUSDT",
        side="LONG",
        order_type="LIMIT",
        qty=0.001,
        price=42100.0,
        signal_id=signal_id,
        sub_uid="TEST_SUBUID",
        owner="MAIN_TEACHER",
        strategy_role="SMOKETEST_ROLE",
        exit_profile="STANDARD_5TP",
        reduce_only=False,
        post_only=True,
        extra={"note": "dummy order from ai_smoketest"},
    )
    print("  -> dummy order logged.")

    # 3) Log a fake closed trade summary
    print("\n[3/3] Logging dummy trade summary...")
    now_ms = int(time.time() * 1000)
    opened_ms = now_ms - 60_000  # pretend trade opened 1 min ago
    trade_id = f"TEST_TRADE_{now_ms}"

    log_trade_summary_basic(
        trade_id=trade_id,
        sub_uid="TEST_SUBUID",
        symbol="BTCUSDT",
        side="LONG",
        opened_ts_ms=opened_ms,
        closed_ts_ms=now_ms,
        outcome="WIN",
        r_multiple=1.5,
        realized_pnl=15.0,
        signal_id=signal_id,
        owner="MAIN_TEACHER",
        strategy_role="SMOKETEST_ROLE",
        entry_price=42000.0,
        exit_price=42300.0,
        max_favorable_excursion_r=2.0,
        max_adverse_excursion_r=-0.3,
        holding_ms=now_ms - opened_ms,
        exit_reason="TP",
        regime_at_entry="{'tags': ['smoketest_entry']}",
        regime_at_exit="{'tags': ['smoketest_exit']}",
        extra={"note": "dummy trade summary from ai_smoketest"},
    )
    print(f"  -> trade_id: {trade_id}")

    # 4) Print simple counts
    print("\n[VERIFY] Counting rows in ai_* tables...")
    conn = _connect()
    cur = conn.cursor()

    for table in ["ai_signals", "ai_orders", "ai_trades"]:
        try:
            cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
            row = cur.fetchone()
            print(f"  {table}: {row['n']} rows")
        except Exception as e:
            print(f"  {table}: ERROR -> {type(e).__name__}: {e}")

    conn.close()

    print("\n=== AI Store Smoke Test completed ===")
    print("If you see non-zero counts above, the AI event store is working.")


if __name__ == "__main__":
    main()
