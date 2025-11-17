# app/bots/equity_drip_bot.py
# Flashback — Equity Drip Bot (Main, dedicated TG, per-fill)
#
# Purpose
#   After each profitable LIMIT fill (realisedPnl > 0) on MAIN unified account,
#   transfer DRIP_PCT of that realised PnL (USDT) from Main UNIFIED
#   -> Subaccount UNIFIED, rotating through SUB_UIDS_ROUND_ROBIN.
#
# Guarantees
#   - Idempotent per execution: processed IDs are remembered on disk.
#   - Respects MAIN_BAL_FLOOR_USD: will skip if floor would be violated.
#   - Skips tiny transfers below DRIP_MIN_USD.
#
# Requirements
#   - SUB_UIDS_ROUND_ROBIN in .env as comma-separated Bybit MemberId UIDs
#   - Keys for inter-transfer set in flashback_common (KEY_XFER/SEC_XFER)
#
# Notes
#   - Polls v5 execution/list every few seconds; only LIMIT, profitable executions.
#   - Only linear USDT category is considered.
#   - Uses dedicated Telegram channel "drip" via app.core.notifier_bot.get_notifier("drip").
#       .env should have something like:
#           TG_TOKEN_DRIP=...
#           TG_CHAT_DRIP=...

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import time
import hashlib
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from pathlib import Path
from typing import Dict, List, Any

import orjson

from app.core.config import settings
from app.core.logger import get_logger
from app.core.flashback_common import (
    bybit_get,
    get_equity_usdt,
    inter_transfer_usdt_to_sub,
    SUB_UIDS_ROUND_ROBIN,
    DRIP_PCT,
    DRIP_MIN_USD,
    MAIN_BAL_FLOOR_USD,
)
from app.core.notifier_bot import get_notifier

CATEGORY = "linear"
POLL_SECONDS = 6
PAGE_LIMIT = 200  # last N executions per poll

# Root-based state path: ROOT/state/drip_state.json
ROOT_DIR: Path = settings.ROOT
STATE_DIR = ROOT_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "drip_state.json"

# Logger for this bot
log = get_logger("equity_drip_bot")

# Dedicated Telegram channel for drip
# Configure "drip" channel in notifier_bot (e.g., TG_TOKEN_DRIP / TG_CHAT_DRIP in .env)
tg = get_notifier("drip")


def _fmt_usd(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


def _round_down_cents(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def _load_state() -> dict:
    """
    State format:
      {
        "processed": { exec_key: true, ... },
        "rr_index": int,
        "last_exec_time_ms": int,
        "last_exec_id": str
      }
    """
    try:
        if STATE_PATH.exists():
            d = orjson.loads(STATE_PATH.read_bytes())
            if isinstance(d, dict):
                d.setdefault("processed", {})
                d.setdefault("rr_index", 0)
                d.setdefault("last_exec_time_ms", 0)
                d.setdefault("last_exec_id", "")
                return d
    except Exception as e:
        log.warning("Drip state load failed, starting fresh: %r", e)
    return {
        "processed": {},
        "rr_index": 0,
        "last_exec_time_ms": 0,
        "last_exec_id": "",
    }


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(state))


def _stable_key(row: dict) -> str:
    """
    Build a deterministic key for an execution row to avoid double-processing.
    """
    fields = [
        str(row.get("symbol", "")),
        str(row.get("side", "")),
        str(row.get("orderId", "")),
        str(row.get("execId", "")),
        str(row.get("execTime", "")),
        str(row.get("orderType", "")),
        str(row.get("execPrice", "")),
        str(row.get("execQty", "")),
        str(row.get("realisedPnl", "")),
    ]
    s = "|".join(fields).encode()
    return hashlib.sha1(s).hexdigest()


def _pick_next_uid(state: dict, uids: List[str]) -> str:
    i = state.get("rr_index", 0) % len(uids)
    uid = uids[i]
    state["rr_index"] = (i + 1) % len(uids)
    return uid


def _effective_drip_pct() -> Decimal:
    """
    Normalize DRIP_PCT to a fraction:
      - If DRIP_PCT <= 1      -> treat as fraction (0.10 -> 10%)
      - If DRIP_PCT > 1       -> treat as percent (10 -> 10%)
    """
    try:
        raw = Decimal(str(DRIP_PCT))
    except (InvalidOperation, TypeError, ValueError):
        log.warning("Invalid DRIP_PCT=%r, defaulting to 0.10", DRIP_PCT)
        raw = Decimal("0.10")

    if raw <= 0:
        return Decimal("0")

    if raw <= 1:
        return raw

    # e.g. 10 -> 0.10
    return (raw / Decimal("100")).quantize(Decimal("0.00000001"))


def _list_recent_executions_since(last_exec_time_ms: int) -> List[dict]:
    """
    Fetch recent executions from Bybit v5 execution/list, linear category.
    We filter client-side for:
      - orderType == "Limit"
      - realisedPnl > 0
    """
    params: Dict[str, str] = {
        "category": CATEGORY,
        "limit": str(PAGE_LIMIT),
        "order": "Asc",
    }
    if last_exec_time_ms > 0:
        # startTime is in ms
        params["startTime"] = str(last_exec_time_ms + 1)

    try:
        r = bybit_get("/v5/execution/list", params)
        rows = r.get("result", {}).get("list", []) or []
        if not isinstance(rows, list):
            return []
    except Exception as e:
        msg = f"[Drip] execution/list fetch error: {e}"
        log.error(msg)
        try:
            tg.error(msg)
        except Exception:
            print(msg)
        return []

    # Sort by execTime ascending just in case
    def _ts(row: dict) -> int:
        t = row.get("execTime") or row.get("createdTime") or "0"
        try:
            return int(t)
        except Exception:
            return 0

    rows.sort(key=_ts)
    return rows


def _is_profitable_limit_fill(row: dict) -> bool:
    """
    Detect a TP-style profitable LIMIT execution.

    Conditions:
      - orderType == "Limit"
      - realisedPnl > 0
      - execQty > 0
    """
    otype = str(row.get("orderType", "")).lower()
    if otype != "limit":
        return False

    try:
        realised = Decimal(str(row.get("realisedPnl", "0") or "0"))
    except Exception:
        return False

    if realised <= 0:
        return False

    try:
        qty = Decimal(str(row.get("execQty", "0") or "0"))
    except Exception:
        qty = Decimal("0")

    if qty <= 0:
        return False

    return True


def _notify_startup(eff_pct: Decimal, subs: List[str]) -> None:
    """
    Send a clear startup heartbeat so you know the bot is alive
    when run individually or under supervisor.
    """
    msg = (
        "💧 Flashback Equity Drip Bot ONLINE\n"
        f"State file: {STATE_PATH}\n"
        f"Poll: {POLL_SECONDS}s | CATEGORY={CATEGORY}\n"
        f"DRIP_PCT raw={DRIP_PCT} → effective={eff_pct}\n"
        f"Round-robin UIDs ({len(subs)}): {', '.join(subs)}"
    )
    log.info(msg.replace("\n", " | "))
    try:
        tg.info(msg)
    except Exception as e:
        # If Telegram wiring is broken, at least you see it in logs/console.
        log.warning("Failed to send DRIP startup Telegram: %r", e)
        print(msg)


def loop() -> None:
    eff_pct = _effective_drip_pct()
    state = _load_state()
    processed: Dict[str, bool] = state.get("processed", {})
    subs = [u.strip() for u in SUB_UIDS_ROUND_ROBIN.split(",") if u.strip()]

    if not subs:
        msg = "💧 Drip disabled: no SUB_UIDS_ROUND_ROBIN configured."
        log.warning(msg)
        try:
            tg.warn(msg)
        except Exception:
            print(msg)
        return

    if eff_pct <= 0:
        msg = "💧 Drip disabled: effective DRIP_PCT <= 0."
        log.warning(msg)
        try:
            tg.warn(msg)
        except Exception:
            print(msg)
        return

    # Clear heartbeat so you can verify it's alive when run manually
    _notify_startup(eff_pct, subs)

    last_exec_time_ms: int = int(state.get("last_exec_time_ms", 0))
    last_exec_id: str = str(state.get("last_exec_id", ""))

    while True:
        try:
            rows = _list_recent_executions_since(last_exec_time_ms)

            if not rows:
                time.sleep(POLL_SECONDS)
                continue

            for row in rows:
                try:
                    # Update cursor baseline from raw timestamps / execId
                    exec_time_raw = row.get("execTime") or row.get("createdTime") or "0"
                    try:
                        exec_time_ms = int(exec_time_raw)
                    except Exception:
                        exec_time_ms = last_exec_time_ms

                    exec_id = str(row.get("execId", "") or row.get("id", ""))

                    # Keep cursor moving forward regardless of whether we drip
                    if exec_time_ms > last_exec_time_ms or (
                        exec_time_ms == last_exec_time_ms and exec_id > last_exec_id
                    ):
                        last_exec_time_ms = exec_time_ms
                        last_exec_id = exec_id

                    key = _stable_key(row)
                    if processed.get(key):
                        continue

                    # Mark as seen eventually (even if we skip) to avoid growing unbounded
                    processed[key] = True

                    if not _is_profitable_limit_fill(row):
                        continue

                    try:
                        pnl = Decimal(str(row.get("realisedPnl", "0") or "0"))
                    except Exception:
                        continue

                    if pnl <= 0:
                        continue

                    amt = _round_down_cents(pnl * eff_pct)
                    if amt < Decimal(str(DRIP_MIN_USD)):
                        # Too small to bother
                        continue

                    # Equity floor check
                    try:
                        eq = get_equity_usdt()
                    except Exception as ge:
                        msg = f"[Drip] get_equity_usdt error: {ge}"
                        log.error(msg)
                        try:
                            tg.error(msg)
                        except Exception:
                            print(msg)
                        continue

                    if (Decimal(str(eq)) - amt) < Decimal(str(MAIN_BAL_FLOOR_USD)):
                        # Respect equity floor, but log the attempt
                        msg = (
                            f"💧 Skipped drip {_fmt_usd(amt)} (floor {MAIN_BAL_FLOOR_USD} "
                            f"would be violated). PnL={_fmt_usd(pnl)}"
                        )
                        log.info(msg)
                        try:
                            tg.info(msg)
                        except Exception:
                            print(msg)
                        continue

                    # Select target sub by round-robin
                    uid = _pick_next_uid(state, subs)

                    # Execute transfer (live)
                    inter_transfer_usdt_to_sub(uid, amt)

                    # Persist state after successful transfer
                    sym = row.get("symbol", "")
                    exit_px = row.get("execPrice", None)
                    side = row.get("side", "")
                    msg = (
                        f"✅ Dripped {_fmt_usd(amt)} from {sym} {side} LIMIT fill "
                        f"PnL={_fmt_usd(pnl)} -> sub UID {uid}"
                        + (f" | execPrice={exit_px}" if exit_px else "")
                    )
                    log.info(msg)
                    try:
                        tg.info(msg)
                    except Exception:
                        print(msg)

                    _save_state(
                        {
                            "processed": processed,
                            "rr_index": state.get("rr_index", 0),
                            "last_exec_time_ms": last_exec_time_ms,
                            "last_exec_id": last_exec_id,
                        }
                    )

                except Exception as row_err:
                    # Row-level errors should not kill the loop
                    msg = f"[Drip] row error: {row_err}"
                    log.warning(msg)
                    try:
                        tg.error(msg)
                    except Exception:
                        print(msg)

            # Periodic state sync even if nothing transferred
            _save_state(
                {
                    "processed": processed,
                    "rr_index": state.get("rr_index", 0),
                    "last_exec_time_ms": last_exec_time_ms,
                    "last_exec_id": last_exec_id,
                }
            )

            time.sleep(POLL_SECONDS)

        except Exception as e:
            msg = f"[Drip] loop error: {e}"
            log.error(msg)
            try:
                tg.error(msg)
            except Exception:
                print(msg)
            time.sleep(8)


if __name__ == "__main__":
    log.info("Equity Drip Bot starting in standalone mode...")
    loop()
