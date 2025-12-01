#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Signal Test Emitter

Purpose:
    - Append simple test signals into signals/observed.jsonl
      so the Auto Executor pipeline can be tested end-to-end
      without touching the real Signal Engine yet.

What it does:
    - Uses EXEC_SIGNALS_PATH from .env (default: signals/observed.jsonl)
    - Emits one or more signals with basic fields:
        symbol, side, reason, ts_ms, est_rr, optional sub_uid
    - Prints to console what it wrote.

Usage examples:
    # 1) Default BTCUSDT Buy test
    python -m app.bots.signal_test_emitter

    # 2) Custom symbol/side via env:
    set TEST_SYMBOL=ETHUSDT
    set TEST_SIDE=Sell
    python -m app.bots.signal_test_emitter

Env (optional):
    EXEC_SIGNALS_PATH  -> path to JSONL file (shared with auto_executor)
    TEST_SYMBOL        -> default "BTCUSDT"
    TEST_SIDE          -> default "Buy"
    TEST_REASON        -> default "manual_test"
    TEST_EST_RR        -> default "0.3"
    TEST_SUB_UID       -> default: uses EXEC_SUB_UID if set, else None
    TEST_COUNT         -> how many identical signals to emit (default 1)
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, Any


ROOT = Path(__file__).resolve().parents[2]

EXEC_SIGNALS_PATH = Path(
    os.getenv("EXEC_SIGNALS_PATH", "signals/observed.jsonl")
)

TEST_SYMBOL = os.getenv("TEST_SYMBOL", "BTCUSDT").strip() or "BTCUSDT"
TEST_SIDE = os.getenv("TEST_SIDE", "Buy").strip() or "Buy"  # "Buy" or "Sell"
TEST_REASON = os.getenv("TEST_REASON", "manual_test").strip() or "manual_test"
TEST_EST_RR = os.getenv("TEST_EST_RR", "0.3").strip() or "0.3"
TEST_SUB_UID = os.getenv("TEST_SUB_UID", os.getenv("EXEC_SUB_UID", "")).strip() or None

try:
    TEST_COUNT = int(os.getenv("TEST_COUNT", "1"))
except Exception:
    TEST_COUNT = 1


def _ensure_signals_path(path: Path) -> None:
    """
    Ensure parent directory exists and file is present (create if missing).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.touch()


def _build_signal() -> Dict[str, Any]:
    """
    Build a minimal signal dict that the auto_executor understands.
    """
    ts_ms = int(time.time() * 1000)

    try:
        est_rr = float(TEST_EST_RR)
    except Exception:
        est_rr = 0.3

    sig: Dict[str, Any] = {
        "symbol": TEST_SYMBOL,
        "side": TEST_SIDE,
        "reason": TEST_REASON,
        "ts_ms": ts_ms,
        "est_rr": est_rr,
    }

    if TEST_SUB_UID:
        sig["sub_uid"] = TEST_SUB_UID

    return sig


def emit_once() -> Dict[str, Any]:
    """
    Emit a single signal line into EXEC_SIGNALS_PATH and return the dict.
    """
    _ensure_signals_path(EXEC_SIGNALS_PATH)
    sig = _build_signal()

    line = json.dumps(sig, separators=(",", ":"), ensure_ascii=False)
    with EXEC_SIGNALS_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    print(
        f"[SIG_EMIT] Wrote signal to {EXEC_SIGNALS_PATH} -> {line}",
        flush=True,
    )
    return sig


def main() -> None:
    print(
        f"Flashback Signal Test Emitter\n"
        f"  EXEC_SIGNALS_PATH = {EXEC_SIGNALS_PATH}\n"
        f"  TEST_SYMBOL       = {TEST_SYMBOL}\n"
        f"  TEST_SIDE         = {TEST_SIDE}\n"
        f"  TEST_REASON       = {TEST_REASON}\n"
        f"  TEST_EST_RR       = {TEST_EST_RR}\n"
        f"  TEST_SUB_UID      = {TEST_SUB_UID or 'None'}\n"
        f"  TEST_COUNT        = {TEST_COUNT}\n",
        flush=True,
    )

    for i in range(TEST_COUNT):
        emit_once()
        if TEST_COUNT > 1:
            time.sleep(0.2)  # small spacing
    print("Done emitting test signal(s).", flush=True)


if __name__ == "__main__":
    main()
