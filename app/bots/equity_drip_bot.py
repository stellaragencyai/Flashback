# app/bots/equity_drip_bot.py
# Flashback — Equity Drip Bot (Main, dedicated TG)
#
# Purpose
#   After each profitable closed trade, transfer DRIP_PCT of realized PnL (USDT)
#   from Main UNIFIED -> Subaccount UNIFIED, rotating through SUB_UIDS_ROUND_ROBIN.
#
# Guarantees
#   - Idempotent per closed trade: processed keys are remembered on disk.
#   - Respects MAIN_BAL_FLOOR_USD: will skip or downsize if floor would be violated.
#   - Skips tiny transfers below DRIP_MIN_USD.
#
# Requirements
#   - SUB_UIDS_ROUND_ROBIN in .env as comma-separated Bybit MemberId UIDs
#   - Keys for inter-transfer set in flashback_common (KEY_XFER/SEC_XFER)
#
# Notes
#   - Polls v5 closed-pnl every few seconds; builds a stable key per row.
#   - Only linear USDT category is considered.
#   - Uses dedicated Telegram channel "drip" via app.core.notifier_bot.get_notifier("drip").
#       .env should have something like:
#           TG_TOKEN_DRIP=...
#           TG_CHAT_DRIP=...

import time
import hashlib
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import orjson

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
PAGE_LIMIT = 100  # last 100 closed pnl rows

# Root-based state path: state/drip_state.json
ROOT_DIR = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "drip_state.json"

# Dedicated Telegram channel for drip
# Configure "drip" channel in notifier_bot (e.g., TG_TOKEN_DRIP / TG_CHAT_DRIP in .env)
tg = get_notifier("drip")


def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {"processed": {}, "rr_index": 0}


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(state))


def _stable_key(row: dict) -> str:
    """
    Build a deterministic key for a closed-pnl row to avoid double-processing.
    We combine several stable-ish fields and hash them.
    """
    fields = [
        str(row.get("symbol", "")),
        str(row.get("side", "")),
        str(row.get("orderId", "")),
        str(row.get("execId", "")),
        str(row.get("createdTime", "")),
        str(row.get("updatedTime", "")),
        str(row.get("avgEntryPrice", "")),
        str(row.get("avgExitPrice", "")),
        str(row.get("closedPnl", "")),
        str(row.get("closedSize", "")),
    ]
    s = "|".join(fields).encode()
    return hashlib.sha1(s).hexdigest()


def _list_recent_closed_pnl() -> List[dict]:
    try:
        r = bybit_get("/v5/position/closed-pnl", {"category": CATEGORY, "limit": str(PAGE_LIMIT)})
        rows = r.get("result", {}).get("list", []) or []
        return rows
    except Exception as e:
        try:
            tg.error(f"[Drip] closed-pnl fetch error: {e}")
        except Exception:
            print(f"[Drip] closed-pnl fetch error: {e}")
        return []


def _fmt(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


def _round_down_cents(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def _pick_next_uid(state: dict, uids: List[str]) -> str:
    i = state.get("rr_index", 0) % len(uids)
    uid = uids[i]
    state["rr_index"] = (i + 1) % len(uids)
    return uid


def loop():
    # Clear startup confirmation from dedicated drip bot
    try:
        tg.info(
            "💧 Flashback Equity Drip Bot started (via supervisor).\n"
            f"State file: {STATE_PATH}\n"
            f"Poll: {POLL_SECONDS}s | CATEGORY={CATEGORY}"
        )
    except Exception:
        print("💧 Flashback Equity Drip Bot started (via supervisor).")

    state = _load_state()
    processed = state.get("processed", {})
    subs = [u.strip() for u in SUB_UIDS_ROUND_ROBIN.split(",") if u.strip()]

    if not subs:
        try:
            tg.warn("💧 Drip disabled: no SUB_UIDS_ROUND_ROBIN configured.")
        except Exception:
            print("💧 Drip disabled: no SUB_UIDS_ROUND_ROBIN configured.")
        return

    while True:
        try:
            rows = _list_recent_closed_pnl()
            if not rows:
                time.sleep(POLL_SECONDS)
                continue

            for row in rows:
                try:
                    key = _stable_key(row)
                    if processed.get(key):
                        continue

                    pnl = Decimal(str(row.get("closedPnl", "0") or "0"))
                    if pnl <= 0:
                        processed[key] = True
                        continue

                    # Amount to drip
                    # If DRIP_PCT is already a fraction (0.15), this is pnl * 0.15.
                    # If DRIP_PCT is 15, we still get reasonable behavior via / Decimal(1) branch.
                    if DRIP_PCT > 1:
                        amt = _round_down_cents(pnl * DRIP_PCT / Decimal(1))
                    else:
                        amt = _round_down_cents(pnl * DRIP_PCT)

                    if amt < Decimal(DRIP_MIN_USD):
                        processed[key] = True
                        continue

                    eq = get_equity_usdt()
                    if (eq - amt) < Decimal(MAIN_BAL_FLOOR_USD):
                        try:
                            tg.info(
                                f"💧 Skipped drip {_fmt(amt)} (floor {MAIN_BAL_FLOOR_USD} would be violated)."
                            )
                        except Exception:
                            print(
                                f"💧 Skipped drip {_fmt(amt)} (floor {MAIN_BAL_FLOOR_USD} would be violated)."
                            )
                        processed[key] = True
                        continue

                    # Select target sub by round-robin
                    uid = _pick_next_uid(state, subs)

                    # Execute transfer (live)
                    inter_transfer_usdt_to_sub(uid, amt)
                    processed[key] = True
                    _save_state({"processed": processed, "rr_index": state["rr_index"]})

                    # Notify
                    sym = row.get("symbol", "")
                    exit_px = row.get("avgExitPrice", None)
                    msg = (
                        f"✅ Dripped {_fmt(amt)} from {sym} profit to sub UID {uid}"
                        + (f" | exit {exit_px}" if exit_px else "")
                    )
                    try:
                        tg.info(msg)
                    except Exception:
                        print(msg)

                except Exception as ie:
                    # Mark as processed on hard parse failures to avoid clogging
                    try:
                        processed[_stable_key(row)] = True
                    except Exception:
                        pass
                    try:
                        tg.error(f"[Drip] row error: {ie}")
                    except Exception:
                        print(f"[Drip] row error: {ie}")

            # Persist state periodically even if nothing changed
            _save_state({"processed": processed, "rr_index": state.get("rr_index", 0)})

            time.sleep(POLL_SECONDS)

        except Exception as e:
            try:
                tg.error(f"[Drip] {e}")
            except Exception:
                print(f"[Drip] {e}")
            time.sleep(8)


if __name__ == "__main__":
    loop()
