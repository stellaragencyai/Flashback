#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Per-Execution Drip via Event Bus (Main)

What this version does
----------------------
- No more polling /v5/position/closed-pnl directly.
- Instead, it consumes EXECUTION events from the normalized event bus:

      state/event_bus.jsonl

- For each NEW profitable LIMIT execution on MAIN:
      • Computes DRIP_PCT of realisedPnl
      • If >= DRIP_MIN_USD and equity > MAIN_BAL_FLOOR_USD:
            transfers that amount from MAIN UNIFIED → next subaccount (round-robin)

- Uses:
      - app.core.event_bus.{load_cursor, save_cursor, read_events}
      - app.core.subs.rr_next()
      - app.core.flashback_common.{get_equity_usdt,
                                    inter_transfer_usdt_to_sub,
                                    DRIP_PCT, DRIP_MIN_USD, MAIN_BAL_FLOOR_USD}
      - Central notifier get_notifier("main")

State files
-----------
- Cursor: state/per_position_drip.cursor
- Internal state: state/per_position_drip_state.json
    {
      "processed": { "<exec_key>": true, ... }
    }

DRY RUN
-------
- DRIP_DRY_RUN=true  -> simulate transfer, log only
- DRIP_DRY_RUN=false -> perform real inter_transfer_usdt_to_sub

Notes
-----
- This is effectively "per-fill" drip, not pure per-position.
- It only reacts to EXECUTION events with:
      orderType == "Limit"
      execQty   > 0
      realisedPnl > 0
  on label == "main".
"""

from __future__ import annotations

import os
import time
import hashlib
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from pathlib import Path
from typing import Optional, Dict, Any, List

import orjson

from app.core.flashback_common import (
    inter_transfer_usdt_to_sub,
    DRIP_PCT,
    DRIP_MIN_USD,
    MAIN_BAL_FLOOR_USD,
    get_equity_usdt,
)
from app.core.subs import rr_next  # peek_current kept for future use if needed
from app.core.notifier_bot import get_notifier

# Event bus (normalized EXECUTION / POSITION stream)
try:
    from app.core import event_bus
except ImportError:  # fallback if app.core isn't the package root
    from core import event_bus  # type: ignore


# --- Paths / config ---

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

STATE_PATH = STATE_DIR / "per_position_drip_state.json"
CURSOR_PATH = STATE_DIR / "per_position_drip.cursor"

POLL_SECONDS = int(os.getenv("DRIP_POLL_SECONDS", "6"))  # how often we poll the event bus


def _parse_bool(val, default: bool = False) -> bool:
    """
    Parse a boolean from env-like strings.
    Accepts: "1", "true", "yes", "on" as True.
    """
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "on")


DRIP_DRY_RUN = _parse_bool(os.getenv("DRIP_DRY_RUN"), True)

tg = get_notifier("main")


# --- Console + Telegram logging wrappers ---


def log_info(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.info(msg)
    except Exception:
        pass


def log_warn(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.warn(msg)
    except Exception:
        pass


def log_error(msg: str) -> None:
    print(msg, flush=True)
    try:
        tg.error(msg)
    except Exception:
        pass


# ---------------- State helpers ---------------- #


def _load_state() -> dict:
    """
    Load drip state (currently only the processed execution keys).
    """
    try:
        if STATE_PATH.exists():
            data = orjson.loads(STATE_PATH.read_bytes())
            if isinstance(data, dict):
                data.setdefault("processed", {})
                return data
    except Exception:
        pass
    return {
        "processed": {},  # exec_key -> True
    }


def _save_state(st: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(st))


# ---------------- Utils ---------------- #


def _fmt_usd(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


def _stable_exec_key(raw: Dict[str, Any]) -> str:
    """
    Build a deterministic key for an EXECUTION event's raw row to avoid re-dripping.

    Uses a few high-entropy fields (symbol, side, orderId, execId, execTime, etc.).
    """
    fields = [
        str(raw.get("symbol", "")),
        str(raw.get("side", "")),
        str(raw.get("orderId", "")),
        str(raw.get("execId", "")),
        str(raw.get("execTime", "")),
        str(raw.get("orderType", "")),
        str(raw.get("execPrice", "")),
        str(raw.get("execQty", "")),
        str(raw.get("realisedPnl", "")),
    ]
    s = "|".join(fields).encode("utf-8")
    return hashlib.sha1(s).hexdigest()


def _is_profitable_limit_fill(raw: Dict[str, Any]) -> bool:
    """
    Detect a TP-style profitable LIMIT execution.

    Conditions:
      - orderType == "Limit"
      - realisedPnl > 0
      - execQty > 0
    """
    otype = str(raw.get("orderType", "")).lower()
    if otype != "limit":
        return False

    try:
        realised = Decimal(str(raw.get("realisedPnl", "0") or "0"))
    except Exception:
        return False

    if realised <= 0:
        return False

    try:
        qty = Decimal(str(raw.get("execQty", "0") or "0"))
    except Exception:
        qty = Decimal("0")

    if qty <= 0:
        return False

    return True


# ---------------- Transfer wrapper ---------------- #


def _drip_transfer_to_sub(uid: str, label: str, amt: Decimal) -> None:
    """
    DRY-aware wrapper around inter_transfer_usdt_to_sub.
    """
    if amt <= 0:
        return

    if DRIP_DRY_RUN:
        log_info(
            f"[Drip][DRY] Would transfer {_fmt_usd(amt)} from MAIN UNIFIED -> {label} ({uid})."
        )
        return

    # Live mode
    inter_transfer_usdt_to_sub(uid, amt)
    log_info(
        f"[Drip] Transferred {_fmt_usd(amt)} from MAIN UNIFIED -> {label} ({uid})."
    )


# ---------------- Main loop (event-bus driven) ---------------- #


def loop() -> None:
    state = _load_state()
    processed: Dict[str, bool] = state.get("processed", {})

    # Cursor for event bus
    cursor_pos: int = event_bus.load_cursor(CURSOR_PATH)

    # Normalize floors & pct as Decimals, with some basic sanity defaults
    try:
        drip_pct = Decimal(str(DRIP_PCT))
    except (InvalidOperation, TypeError, ValueError):
        drip_pct = Decimal("0.10")

    try:
        drip_min = Decimal(str(DRIP_MIN_USD))
    except (InvalidOperation, TypeError, ValueError):
        drip_min = Decimal("1")

    try:
        floor = Decimal(str(MAIN_BAL_FLOOR_USD))
    except (InvalidOperation, TypeError, ValueError):
        floor = Decimal("0")

    log_info(
        "💧 Per-Execution Drip (event-bus) started.\n"
        f"  DRIP_DRY_RUN: {'ON' if DRIP_DRY_RUN else 'OFF'}\n"
        f"  STATE_PATH: {STATE_PATH}\n"
        f"  CURSOR_PATH: {CURSOR_PATH}\n"
        f"  POLL_SECONDS: {POLL_SECONDS}\n"
        f"  DRIP_PCT raw={DRIP_PCT} → effective={drip_pct}\n"
        f"  DRIP_MIN_USD={drip_min} | MAIN_BAL_FLOOR_USD={floor}"
    )

    while True:
        try:
            # 1) Read new EXECUTION events for MAIN from the event bus
            events, new_pos = event_bus.read_events(
                start_pos=cursor_pos,
                allowed_types={"EXECUTION"},
                allowed_labels={"main"},
            )

            if new_pos != cursor_pos:
                cursor_pos = new_pos
                event_bus.save_cursor(CURSOR_PATH, cursor_pos)

            if not events:
                time.sleep(POLL_SECONDS)
                continue

            for ev in events:
                # ev is the normalized event; raw WS row is under "raw"
                raw = ev.get("raw") or {}
                if not raw:
                    continue

                key = _stable_exec_key(raw)
                if processed.get(key):
                    continue  # already dripped for this execution
                processed[key] = True  # mark as seen (even if we skip later)

                if not _is_profitable_limit_fill(raw):
                    continue

                symbol = raw.get("symbol", "?")

                # realisedPnl
                try:
                    pnl = Decimal(str(raw.get("realisedPnl", "0") or "0"))
                except Exception:
                    continue

                if pnl <= 0:
                    continue

                # Equity floor check
                try:
                    eq = get_equity_usdt()
                    eq_dec = Decimal(str(eq))
                except Exception as e:
                    log_warn(f"⚠️ Drip: get_equity_usdt error ({symbol}): {e}")
                    continue

                if eq_dec < floor:
                    log_info(
                        f"🟨 Drip skipped for {symbol} (equity {_fmt_usd(eq_dec)} "
                        f"below floor {_fmt_usd(floor)})."
                    )
                    continue

                amt = (pnl * drip_pct).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if amt < drip_min:
                    log_info(
                        f"ℹ️ Drip too small for {symbol}: "
                        f"would send {_fmt_usd(amt)}, min is {_fmt_usd(drip_min)}."
                    )
                    continue

                sub = rr_next()
                if not sub:
                    log_warn(
                        f"⚠️ Drip: no subaccount available for {symbol} "
                        f"profit {_fmt_usd(pnl)} (rr_next() returned None)."
                    )
                    continue

                uid = sub.get("uid")
                label = sub.get("label", f"sub-{uid}")

                try:
                    _drip_transfer_to_sub(uid, label, amt)
                    if not DRIP_DRY_RUN:
                        log_info(
                            f"✅ Drip: {symbol} profit {_fmt_usd(pnl)} → "
                            f"sent {_fmt_usd(amt)} to {label} ({uid})."
                        )
                except Exception as e:
                    log_warn(
                        f"⚠️ Drip transfer failed to {label} ({uid}) for {_fmt_usd(amt)}: {e}"
                    )

            # Persist updated state (processed keys) periodically
            state["processed"] = processed
            _save_state(state)

            time.sleep(POLL_SECONDS)

        except Exception as e:
            log_error(f"[Drip] loop error: {e}")
            time.sleep(8)


if __name__ == "__main__":
    loop()
