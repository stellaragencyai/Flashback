# app/bots/per_position_drip.py
# Flashback — Per-Position Drip (Main, upgraded)
#
# What it does
# - Polls latest closed PnL entries (linear).
# - For each NEW profitable close (never processed before):
#       • Calculates DRIP_PCT of realized PnL.
#       • If >= DRIP_MIN_USD and equity > MAIN_BAL_FLOOR_USD:
#             transfers that amount to the next subaccount (round-robin).
# - Uses a state file to remember the last processed row id.
#
# Enhancements vs previous version:
#   1) Fixed "last_row_id" logic so it does NOT re-drip old trades.
#   2) Uses central notifier (get_notifier("main")) instead of raw send_tg.
#   3) Safer Decimal handling for equity vs MAIN_BAL_FLOOR_USD / DRIP_MIN_USD.
#   4) Clearer logs & small env-based tuning for polling interval.
#
# Requirements:
#   - DRIP_PCT, DRIP_MIN_USD, MAIN_BAL_FLOOR_USD defined in flashback_common/.env
#   - app.core.subs.rr_next() returns {"uid": ..., "label": ...} or similar.
#   - profit_sweeper and this script can co-exist; they use separate state.

import os
import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Optional, Dict, Any, List

import orjson

from app.core.flashback_common import (
    bybit_get,
    inter_transfer_usdt_to_sub,
    DRIP_PCT,
    DRIP_MIN_USD,
    MAIN_BAL_FLOOR_USD,
    get_equity_usdt,
)
from app.core.subs import rr_next, peek_current  # peek_current kept for future use
from app.core.notifier_bot import get_notifier

tg = get_notifier("main")

STATE_PATH = Path("app/state/drip_state.json")
CATEGORY = "linear"
POLL_SECONDS = int(os.getenv("DRIP_POLL_SECONDS", "6"))  # can tune via .env

# ---------------- State helpers ---------------- #


def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {
        "last_row_id": None,
    }


def _save_state(st: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(st))


# ---------------- PnL / rows ---------------- #


def _latest_closed(limit: int = 5) -> List[dict]:
    """
    Fetch latest closed PnL rows (newest first) for linear category.
    """
    r = bybit_get("/v5/position/closed-pnl", {"category": CATEGORY, "limit": str(limit)})
    return r.get("result", {}).get("list", []) or []


def _row_id(row: Dict[str, Any]) -> str:
    """
    Construct a unique-ish id from symbol + execTime/updatedTime.
    This is used to avoid reprocessing the same close twice.
    """
    symbol = row.get("symbol", "?")
    ts = row.get("execTime", row.get("updatedTime", "0"))
    return f"{symbol}:{ts}"


def _fmt_usd(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


# ---------------- Main loop ---------------- #


def loop() -> None:
    st = _load_state()
    tg.info("💧 Per-Position Drip started.")
    last_row_id: Optional[str] = st.get("last_row_id")

    # Normalize floors & pct as Decimals
    drip_pct = Decimal(str(DRIP_PCT))
    drip_min = Decimal(str(DRIP_MIN_USD))
    floor = Decimal(str(MAIN_BAL_FLOOR_USD))

    while True:
        try:
            rows = _latest_closed(limit=5)
            if not rows:
                time.sleep(POLL_SECONDS)
                continue

            # Bybit returns newest first; we want to process from oldest -> newest
            # but ONLY rows that came after last_row_id.
            new_rows: List[dict] = []
            seen_last = False

            # Iterate from oldest to newest
            for row in reversed(rows):
                rid = _row_id(row)
                if last_row_id is not None and rid == last_row_id:
                    # We've reached the last processed row; everything before this is older.
                    seen_last = True
                    new_rows = []  # discard anything older we might have appended
                    continue
                new_rows.append(row)

            # If last_row_id exists and doesn't appear in the latest batch,
            # we just treat all reversed rows as potentially new (up to 'limit').
            # The above logic already does that naturally when seen_last == False.

            # Process in chronological order (oldest first)
            for row in new_rows:
                rid = _row_id(row)
                pnl = Decimal(str(row.get("closedPnl", "0")))
                sym = row.get("symbol", "?")

                if pnl > 0:
                    eq = get_equity_usdt()

                    if eq < floor:
                        tg.info(
                            f"🟨 Drip skipped for {sym} (equity {_fmt_usd(eq)} "
                            f"below floor {_fmt_usd(floor)})."
                        )
                    else:
                        amt = (pnl * drip_pct).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                        if amt >= drip_min:
                            sub = rr_next()
                            if sub:
                                uid = sub.get("uid")
                                label = sub.get("label", f"sub-{uid}")
                                try:
                                    inter_transfer_usdt_to_sub(uid, amt)
                                    tg.info(
                                        f"✅ Drip: {sym} profit {_fmt_usd(pnl)} → "
                                        f"sent {_fmt_usd(amt)} to {label} ({uid})."
                                    )
                                except Exception as e:
                                    tg.warn(
                                        f"⚠️ Drip transfer failed to {label} ({uid}): {e}"
                                    )
                            else:
                                tg.warn(
                                    f"⚠️ Drip: no subaccount available for {sym} "
                                    f"profit {_fmt_usd(pnl)} (rr_next() returned None)."
                                )
                        else:
                            tg.info(
                                f"ℹ️ Drip too small for {sym}: "
                                f"would send {_fmt_usd(amt)}, min is {_fmt_usd(drip_min)}."
                            )

                # Update last_row_id to the most recent row we've processed
                last_row_id = rid
                st["last_row_id"] = last_row_id
                _save_state(st)

            time.sleep(POLL_SECONDS)

        except Exception as e:
            tg.error(f"[Drip] {e}")
            time.sleep(8)


if __name__ == "__main__":
    loop()
