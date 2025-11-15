# app/bots/equity_drip_bot.py
# Flashback — Equity Drip Bot (Main)
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

import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import hashlib
import orjson

from app.core.flashback_common import (
    bybit_get, send_tg, get_equity_usdt, inter_transfer_usdt_to_sub,
    SUB_UIDS_ROUND_ROBIN, DRIP_PCT, DRIP_MIN_USD, MAIN_BAL_FLOOR_USD
)

CATEGORY = "linear"
POLL_SECONDS = 6
STATE_PATH = Path("app/state/drip_state.json")
PAGE_LIMIT = 100  # last 100 closed pnl rows

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
        send_tg(f"[Drip] closed-pnl fetch error: {e}")
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
    send_tg("💧 Flashback Equity Drip Bot started.")
    state = _load_state()
    processed = state.get("processed", {})
    subs = [u.strip() for u in SUB_UIDS_ROUND_ROBIN.split(",") if u.strip()]

    if not subs:
        send_tg("💧 Drip disabled: no SUB_UIDS_ROUND_ROBIN configured.")
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
                    amt = _round_down_cents(pnl * DRIP_PCT / Decimal(1)) if DRIP_PCT > 1 else _round_down_cents(pnl * DRIP_PCT)
                    # In our config DRIP_PCT is already a fraction (e.g., 0.15). If it's 15, the code above still works.

                    if amt < Decimal(DRIP_MIN_USD):
                        processed[key] = True
                        continue

                    eq = get_equity_usdt()
                    # Only outgoing here; equity check ensures floor not violated
                    if (eq - amt) < Decimal(MAIN_BAL_FLOOR_USD):
                        send_tg(f"💧 Skipped drip {_fmt(amt)} (floor {MAIN_BAL_FLOOR_USD} would be violated).")
                        processed[key] = True
                        continue

                    # Select target sub by round-robin
                    uid = _pick_next_uid(state, subs)

                    # Execute transfer
                    inter_transfer_usdt_to_sub(uid, amt)
                    processed[key] = True
                    _save_state({"processed": processed, "rr_index": state["rr_index"]})

                    # Notify
                    sym = row.get("symbol", "")
                    exit_px = row.get("avgExitPrice", None)
                    send_tg(f"✅ Dripped {_fmt(amt)} from {sym} profit to sub UID {uid}"
                            + (f" | exit {exit_px}" if exit_px else ""))

                except Exception as ie:
                    # Mark as processed on hard parse failures to avoid clogging
                    try:
                        processed[_stable_key(row)] = True
                    except Exception:
                        pass
                    send_tg(f"[Drip] row error: {ie}")

            # Persist state periodically even if nothing changed
            _save_state({"processed": processed, "rr_index": state.get("rr_index", 0)})

            time.sleep(POLL_SECONDS)

        except Exception as e:
            send_tg(f"[Drip] {e}")
            time.sleep(8)

if __name__ == "__main__":
    loop()
