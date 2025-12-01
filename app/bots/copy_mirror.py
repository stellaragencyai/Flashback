#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Copy Mirror (Main → Subaccounts)

What it does
------------
- Polls MAIN account linear positions (USDT perps) every COPY_POLL_SECONDS.
- Keeps an internal "baseline" snapshot of MAIN positions.
- On every loop, computes the CHANGE in position per symbol since last snapshot.
- For each change (delta) it:
    • Converts to signed qty: Buy = +size, Sell = -size.
    • Mirrors that delta to each configured copy subaccount (sub1, sub2, etc.)
      using market orders on linear perps.

Key points
----------
- It mirrors **net position changes**, not individual executions.
- On first boot, it **attaches** to current MAIN positions without trading on subs.
  Copying starts only from subsequent changes (safer).
- Uses:
    BYBIT_MAIN_* keys from flashback_common for reading positions.
    BYBIT_SUB*_TRADE_KEY / BYBIT_SUB*_TRADE_SECRET for trades on subs.
- Controlled via .env:

    COPY_ENABLED=true
    COPY_SUBS=sub1,sub2
    COPY_SCALE_MODE=ratio
    COPY_SCALE_RATIO=1.0
    COPY_POLL_SECONDS=2

State
-----
- app/state/copy_mirror_state.json

    {
      "baseline": {"BTCUSDT": "0.001", "ETHUSDT": "0"},
      "boot_attached": true
    }
"""

from __future__ import annotations

import os
import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import orjson

from app.core.flashback_common import (
    list_open_positions,
    get_ticks,
    qdown,
    bybit_post,
)
from app.core.notifier_bot import get_notifier

# ------------- Config from .env -------------

COPY_ENABLED = os.getenv("COPY_ENABLED", "false").lower() == "true"
COPY_SUBS_RAW = os.getenv("COPY_SUBS", "sub1,sub2")
COPY_SCALE_MODE = os.getenv("COPY_SCALE_MODE", "ratio").lower()
COPY_SCALE_RATIO = Decimal(os.getenv("COPY_SCALE_RATIO", "1.0"))
COPY_POLL_SECONDS = int(os.getenv("COPY_POLL_SECONDS", "2"))

STATE_PATH = Path("app/state/copy_mirror_state.json")

# SUB name → env prefix for trade keys
_SUB_KEY_ENV_MAP: Dict[str, Tuple[str, str]] = {
    f"sub{i}": (f"BYBIT_SUB{i}_TRADE_KEY", f"BYBIT_SUB{i}_TRADE_SECRET")
    for i in range(1, 11)
}

tg = get_notifier("main")


# ------------- Helpers: state -------------

def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {"baseline": {}, "boot_attached": False}


def _save_state(st: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(st))


# ------------- Helpers: MAIN positions -------------

def _signed_size_from_pos(p: Dict[str, Any]) -> Decimal:
    """
    Convert a position row into a signed size:
       Buy  size -> +size
       Sell size -> -size
       Closed / zero size -> 0
    """
    try:
        size = Decimal(str(p.get("size", "0")))
    except Exception:
        size = Decimal("0")
    if size <= 0:
        return Decimal("0")

    side = str(p.get("side", "")).lower()
    if side == "buy":
        return size
    if side == "sell":
        return -size
    return Decimal("0")


def _snapshot_main_positions() -> Dict[str, Decimal]:
    """
    Build symbol → signed_size map for MAIN linear USDT positions.
    """
    out: Dict[str, Decimal] = {}
    try:
        for p in list_open_positions():
            sym = p.get("symbol")
            if not sym:
                continue
            s = _signed_size_from_pos(p)
            if s != 0:
                out[sym] = s
    except Exception as e:
        tg.warn(f"[CopyMirror] Failed to read main positions: {e}")
    return out


# ------------- Helpers: subs & size scaling -------------

def _load_copy_targets() -> List[Dict[str, Any]]:
    """
    Use COPY_SUBS (e.g. 'sub1,sub2') and map to BYBIT_SUBX_TRADE_KEY/SECRET.
    Returns list of:
      {"name": "sub1", "key": "...", "secret": "..."}
    """
    names = [x.strip() for x in COPY_SUBS_RAW.split(",") if x.strip()]
    targets: List[Dict[str, Any]] = []

    for name in names:
        prefix = _SUB_KEY_ENV_MAP.get(name)
        if not prefix:
            tg.warn(f"[CopyMirror] Unknown COPY_SUBS entry '{name}' (no env mapping).")
            continue
        key_env, sec_env = prefix
        key = os.getenv(key_env, "").strip()
        sec = os.getenv(sec_env, "").strip()
        if not key or not sec:
            tg.warn(f"[CopyMirror] Missing trade keys for {name} ({key_env}/{sec_env}).")
            continue
        targets.append({"name": name, "key": key, "secret": sec})

    return targets


def _scale_qty(main_delta: Decimal, symbol: str) -> Decimal:
    """
    Scale main position delta → sub order qty, then snap to lot step.
    Currently only COPY_SCALE_MODE='ratio' supported.
    """
    _, step, _ = get_ticks(symbol)
    if COPY_SCALE_MODE == "ratio":
        raw = main_delta.copy_abs() * COPY_SCALE_RATIO
    else:
        # Fallback: treat everything as ratio if misconfigured
        raw = main_delta.copy_abs()

    if raw <= 0:
        return Decimal("0")

    # Snap down to symbol's qtyStep
    return qdown(raw, step)


# ------------- Core: mirror deltas -------------

def _mirror_delta(symbol: str, main_delta: Decimal, targets: List[Dict[str, Any]]) -> None:
    """
    Mirror a change in MAIN position for one symbol to all COPY targets.

    main_delta:
      > 0  => net increase long (or reduction short) on MAIN
      < 0  => net increase short (or reduction long) on MAIN
    """
    if main_delta == 0 or not targets:
        return

    # Direction of the adjustment for subs
    side = "Buy" if main_delta > 0 else "Sell"
    qty_main_abs = main_delta.copy_abs()

    for t in targets:
        name = t["name"]
        key = t["key"]
        sec = t["secret"]

        qty_sub = _scale_qty(main_delta, symbol)
        if qty_sub <= 0:
            tg.info(
                f"[CopyMirror] Δ {symbol} {side} main={qty_main_abs} but sub {name} "
                f"qty snapped to 0 (too small)."
            )
            continue

        body = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": str(qty_sub),
            "positionIdx": 0,
        }

        try:
            bybit_post("/v5/order/create", body, key=key, secret=sec)
            tg.trade(
                f"🪞 CopyMirror → {name}: {side} {symbol} qty≈{qty_sub} "
                f"(main Δ≈{qty_main_abs})"
            )
        except Exception as e:
            tg.warn(f"[CopyMirror] Failed to mirror {symbol} {side} to {name}: {e}")


# ------------- Main loop -------------

def loop() -> None:
    if not COPY_ENABLED:
        tg.info("🪞 CopyMirror is disabled (COPY_ENABLED=false). Exiting.")
        return

    targets = _load_copy_targets()
    if not targets:
        tg.warn("🪞 CopyMirror has no valid COPY_SUBS targets. Exiting.")
        return

    st = _load_state()
    baseline_raw: Dict[str, str] = st.get("baseline", {}) or {}
    boot_attached: bool = bool(st.get("boot_attached", False))

    # Convert baseline strings → Decimal
    baseline: Dict[str, Decimal] = {}
    for sym, sval in baseline_raw.items():
        try:
            baseline[sym] = Decimal(str(sval))
        except Exception:
            baseline[sym] = Decimal("0")

    tg.info(
        f"🪞 CopyMirror started | COPY_SUBS={COPY_SUBS_RAW} | "
        f"mode={COPY_SCALE_MODE} | ratio={COPY_SCALE_RATIO}"
    )

    while True:
        try:
            main_now = _snapshot_main_positions()

            # Convert to signed decimals
            snap_now: Dict[str, Decimal] = {}
            for sym, val in main_now.items():
                try:
                    snap_now[sym] = Decimal(str(val))
                except Exception:
                    snap_now[sym] = Decimal("0")

            # First run: attach without trading
            if not boot_attached:
                baseline = snap_now.copy()
                st["baseline"] = {k: str(v) for k, v in baseline.items()}
                st["boot_attached"] = True
                _save_state(st)
                tg.info("🪞 CopyMirror attached to current MAIN positions (no backfill). Mirroring from now on.")
                time.sleep(COPY_POLL_SECONDS)
                continue

            # Build symbol universe
            all_syms = set(baseline.keys()) | set(snap_now.keys())

            for sym in sorted(all_syms):
                old = baseline.get(sym, Decimal("0"))
                new = snap_now.get(sym, Decimal("0"))
                delta = new - old
                if delta == 0:
                    continue

                # Mirror this change to all copy subs
                _mirror_delta(sym, delta, targets)

            # Update baseline
            baseline = snap_now
            st["baseline"] = {k: str(v) for k, v in baseline.items()}
            _save_state(st)

            time.sleep(COPY_POLL_SECONDS)

        except Exception as e:
            tg.error(f"[CopyMirror] loop error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    loop()
