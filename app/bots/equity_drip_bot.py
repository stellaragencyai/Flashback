# app/bots/equity_drip_bot.py
# Flashback — Equity Drip Bot (Main, execution bus, per-fill)
#
# v2.2 — execution-bus compatible (execPnl-aware)
#
# Purpose
#   After each profitable fill (P&L > 0) on MAIN unified account,
#   transfer DRIP_PCT of that PnL (USDT) from Main UNIFIED
#   -> Subaccount UNIFIED, rotating through SUB_UIDS_ROUND_ROBIN or a single
#      DRIP_SINGLE_SUB_UID (override).
#
# Data source
#   - Consumes the execution bus:
#         state/ws_executions.jsonl
#     written by ws_switchboard_bot.py:
#         { "label": "...", "ts": ..., "row": { ... Bybit exec row ... } }
#
# Guarantees
#   - Idempotent per execution: processed IDs remembered on disk.
#   - DRIP_MIN_USD and MAIN_BAL_FLOOR_USD can be set to 0 in .env to effectively disable
#     min-size and equity floor constraints.
#
# Key controls (ENV)
#   - DRIP_ENABLED=true/false           -> master on/off switch
#   - DRIP_DRY_RUN=true/false           -> simulate transfers only, no real moves
#   - DRIP_PCT                          -> % of PnL to drip (0.1 or 10 both = 10%)
#   - DRIP_MIN_USD                      -> minimum drip amount (set 0 to disable)
#   - MAIN_BAL_FLOOR_USD                -> do not drip if equity would fall below this
#   - DRIP_SINGLE_SUB_UID               -> if set, all drips go to this UID instead of round-robin
#   - SUB_UIDS_ROUND_ROBIN              -> comma-separated list of UIDs for rotation
#
# Notes
#   - Only executions with label == "main" are considered by default.
#   - Uses dedicated Telegram channel "drip" via get_notifier("drip").

from __future__ import annotations

import os
import time
import hashlib
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from pathlib import Path
from typing import Dict, List, Any

import orjson

from app.core.config import settings
from app.core.logger import get_logger
from app.core.flashback_common import (
    get_equity_usdt,
    inter_transfer_usdt_to_sub,
    SUB_UIDS_ROUND_ROBIN,
    DRIP_PCT,
    DRIP_MIN_USD,
    MAIN_BAL_FLOOR_USD,
)
from app.core.notifier_bot import get_notifier

CATEGORY = "linear"
POLL_SECONDS = 3  # how often we re-scan the execution bus
MAIN_LABEL = os.getenv("DRIP_MAIN_LABEL", "main")  # normally "main"

# Global feature toggles
DRIP_ENABLED = os.getenv("DRIP_ENABLED", "true").strip().lower() == "true"
DRIP_DRY_RUN = os.getenv("DRIP_DRY_RUN", "false").strip().lower() == "true"

ROOT_DIR: Path = settings.ROOT
STATE_DIR = ROOT_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

STATE_PATH = STATE_DIR / "drip_state.json"
CURSOR_PATH = STATE_DIR / "drip.cursor"
WS_EXEC_BUS_PATH = STATE_DIR / "ws_executions.jsonl"

log = get_logger("equity_drip_bot")

# Dedicated Telegram channel for drip
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
        "rr_index": int
      }
    """
    try:
        if STATE_PATH.exists():
            d = orjson.loads(STATE_PATH.read_bytes())
            if isinstance(d, dict):
                d.setdefault("processed", {})
                d.setdefault("rr_index", 0)
                # legacy keys from older versions; harmless if present
                d.pop("last_exec_time_ms", None)
                d.pop("last_exec_id", None)
                return d
    except Exception as e:
        log.warning("Drip state load failed, starting fresh: %r", e)
    return {
        "processed": {},
        "rr_index": 0,
    }


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(state))


def _load_cursor() -> int:
    """
    Byte offset into ws_executions.jsonl so we can resume without rereading.
    """
    try:
        if CURSOR_PATH.exists():
            raw = CURSOR_PATH.read_text().strip()
            return int(raw or "0")
    except Exception:
        pass
    return 0


def _save_cursor(pos: int) -> None:
    try:
        CURSOR_PATH.write_text(str(pos))
    except Exception as e:
        log.warning("Failed to save drip cursor at %s: %r", pos, e)


def _stable_key(row: dict) -> str:
    """
    Build a deterministic key for an execution row to avoid double-processing.

    Uses both realisedPnl and execPnl so it works with either field name.
    """
    pnl_raw = row.get("realisedPnl")
    if pnl_raw is None:
        pnl_raw = row.get("execPnl", "")

    fields = [
        str(row.get("symbol", "")),
        str(row.get("side", "")),
        str(row.get("orderId", "")),
        str(row.get("execId", "")),
        str(row.get("execTime", "")),
        str(row.get("orderType", "")),
        str(row.get("execPrice", "")),
        str(row.get("execQty", "")),
        str(pnl_raw),
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


def _extract_pnl(row: dict) -> Decimal:
    """
    Unified PnL extractor:
      - Prefer realisedPnl if present
      - Otherwise fall back to execPnl
    """
    raw = row.get("realisedPnl")
    if raw is None:
        raw = row.get("execPnl")

    if raw is None:
        return Decimal("0")

    try:
        return Decimal(str(raw or "0"))
    except Exception:
        return Decimal("0")


def _is_profitable_fill(row: dict) -> bool:
    """
    Detect a profitable execution we want to drip from.

    Conditions:
      - PnL > 0 (realisedPnl or execPnl)
      - execQty > 0
      - Accept both Limit and Market fills (TPs or manual exits).
    """
    pnl = _extract_pnl(row)
    if pnl <= 0:
        return False

    try:
        qty = Decimal(str(row.get("execQty", "0") or "0"))
    except Exception:
        qty = Decimal("0")

    if qty <= 0:
        return False

    otype = str(row.get("orderType", "")).lower()
    if otype not in ("limit", "market"):
        # ignore weird types
        return False

    return True


def _notify_startup(eff_pct: Decimal, subs: List[str], mode_desc: str) -> None:
    """
    Send a clear startup heartbeat so you know the bot is alive
    when run individually or under supervisor.
    """
    try:
        pct_str = (eff_pct * Decimal("100")).quantize(Decimal("0.01"))
    except Exception:
        pct_str = eff_pct * Decimal("100")

    msg = (
        "💧 Flashback Equity Drip Bot ONLINE\n"
        f"Mode: {mode_desc}\n"
        f"Main label: {MAIN_LABEL}\n"
        f"Drip rate: {pct_str}% of positive PnL per fill\n"
        f"Execution bus: {WS_EXEC_BUS_PATH}\n"
        f"Cursor file: {CURSOR_PATH}\n"
        f"Poll: {POLL_SECONDS}s | CATEGORY={CATEGORY}\n"
        f"DRIP_ENABLED={DRIP_ENABLED} | DRIP_DRY_RUN={DRIP_DRY_RUN}\n"
        f"Target UIDs ({len(subs)}): {', '.join(subs)}"
    )
    log.info(msg.replace("\n", " | "))
    try:
        tg.info(msg)
    except Exception as e:
        log.warning("Failed to send DRIP startup Telegram: %r", e)
        print(msg)


def _process_one_execution(
    label: str,
    row: Dict[str, Any],
    state: dict,
    processed: Dict[str, bool],
    eff_pct: Decimal,
    subs: List[str],
) -> None:
    """
    Process a single execution row (from execution bus envelope).
    Only drips if:
      - label == MAIN_LABEL
      - profitable fill (PnL > 0, qty > 0)
      - drip amount >= DRIP_MIN_USD
      - equity floor respected
      - DRIP_ENABLED is true
    """
    if label != MAIN_LABEL:
        return

    key = _stable_key(row)
    if processed.get(key):
        return

    # Mark as processed immediately so we don't hammer same fill on errors
    processed[key] = True

    if not DRIP_ENABLED:
        # Intentionally do nothing, just keep state moving
        return

    if not _is_profitable_fill(row):
        return

    pnl = _extract_pnl(row)
    if pnl <= 0:
        return

    amt = _round_down_cents(pnl * eff_pct)
    try:
        min_usd = Decimal(str(DRIP_MIN_USD))
    except Exception:
        min_usd = Decimal("0")

    if amt < min_usd:
        # Too small to bother
        return

    # Equity floor check
    try:
        eq = Decimal(str(get_equity_usdt()))
    except Exception as ge:
        msg = f"[Drip] get_equity_usdt error: {ge}"
        log.error(msg)
        try:
            tg.error(msg)
        except Exception:
            print(msg)
        return

    try:
        floor = Decimal(str(MAIN_BAL_FLOOR_USD))
    except Exception:
        floor = Decimal("0")

    if (eq - amt) < floor:
        msg = (
            f"💧 Skipped drip {_fmt_usd(amt)} (floor {MAIN_BAL_FLOOR_USD} "
            f"would be violated). PnL={_fmt_usd(pnl)}"
        )
        log.info(msg)
        try:
            tg.info(msg)
        except Exception:
            print(msg)
        return

    # Select target sub: either single-sub or round-robin
    uid = _pick_next_uid(state, subs)

    sym = row.get("symbol", "")
    exit_px = row.get("execPrice", None)
    side = row.get("side", "")

    # Handle dry-run vs live mode
    if DRIP_DRY_RUN:
        msg = (
            f"🧪 DRY-RUN drip {_fmt_usd(amt)} from {sym} {side} fill "
            f"PnL={_fmt_usd(pnl)} -> sub UID {uid}"
            + (f" | execPrice={exit_px}" if exit_px else "")
        )
        log.info(msg)
        try:
            tg.info(msg)
        except Exception:
            print(msg)
        return

    # Execute transfer (live)
    try:
        inter_transfer_usdt_to_sub(uid, amt)
    except Exception as tx_err:
        msg = f"[Drip] transfer to UID {uid} for {_fmt_usd(amt)} FAILED: {tx_err}"
        log.error(msg)
        try:
            tg.error(msg)
        except Exception:
            print(msg)
        return

    # Success
    msg = (
        f"✅ Dripped {_fmt_usd(amt)} from {sym} {side} fill "
        f"PnL={_fmt_usd(pnl)} -> sub UID {uid}"
        + (f" | execPrice={exit_px}" if exit_px else "")
    )
    log.info(msg)
    try:
        tg.info(msg)
    except Exception:
        print(msg)


def loop() -> None:
    eff_pct = _effective_drip_pct()
    state = _load_state()
    processed: Dict[str, bool] = state.get("processed", {})

    # single-sub override (e.g. send everything to flashback10)
    single_uid = os.getenv("DRIP_SINGLE_SUB_UID", "").strip()

    if single_uid:
        subs = [single_uid]
        mode_desc = f"single-sub (DRIP_SINGLE_SUB_UID={single_uid})"
    else:
        subs = [u.strip() for u in SUB_UIDS_ROUND_ROBIN.split(",") if u.strip()]
        mode_desc = f"round-robin (SUB_UIDS_ROUND_ROBIN={SUB_UIDS_ROUND_ROBIN})"

    if not subs:
        msg = "💧 Drip disabled: no target UIDs configured (subs list is empty)."
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

    _notify_startup(eff_pct, subs, mode_desc)

    pos = _load_cursor()

    while True:
        try:
            if not WS_EXEC_BUS_PATH.exists():
                # If switchboard isn't running yet, just idle
                time.sleep(POLL_SECONDS)
                continue

            file_size = WS_EXEC_BUS_PATH.stat().st_size
            if pos > file_size:
                # File got rotated or truncated; reset cursor
                log.info(
                    "[Drip] ws_executions.jsonl truncated (size=%s, cursor=%s). "
                    "Resetting cursor to 0.",
                    file_size,
                    pos,
                )
                pos = 0
                _save_cursor(pos)

            with WS_EXEC_BUS_PATH.open("rb") as f:
                f.seek(pos)
                for raw in f:
                    pos = f.tell()
                    try:
                        line = raw.decode("utf-8").strip()
                    except Exception as e:
                        log.warning("[Drip] failed to decode line at pos=%s: %r", pos, e)
                        continue

                    if not line:
                        continue

                    try:
                        env = orjson.loads(line)
                    except Exception as e:
                        log.warning("[Drip] invalid JSON at pos=%s: %r", pos, e)
                        continue

                    label = env.get("label")
                    row = env.get("row") or {}
                    if not isinstance(row, dict):
                        continue

                    _process_one_execution(
                        label=label or "",
                        row=row,
                        state=state,
                        processed=processed,
                        eff_pct=eff_pct,
                        subs=subs,
                    )

                # After scanning all new lines, persist cursor + state
                _save_cursor(pos)
                _save_state(
                    {
                        "processed": processed,
                        "rr_index": state.get("rr_index", 0),
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
    log.info("Equity Drip Bot starting in standalone mode (execution bus consumer)...")
    while True:
        try:
            loop()
        except Exception as e:
            msg = f"[Drip] FATAL: loop() crashed: {e}"
            log.error(msg)
            print(msg)
            time.sleep(10)
