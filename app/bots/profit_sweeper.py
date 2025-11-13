# app/bots/profit_sweeper.py
# Flashback — Profit Sweeper (Main, upgraded)
#
# What it does
# - At your daily cutoff (London time), compute today's realized PnL (USDT) on the main account.
# - If PnL > 0:
#       Allocate per SWEEP_ALLOCATION (e.g., "60:MAIN,25:FUNDING,15:SUBS").
#       • MAIN     -> no transfer; remains in main
#       • FUNDING  -> UNIFIED -> FUNDING internal transfer
#       • SUBS     -> round-robin UNIFIED -> sub UNIFIED (by MemberId UID list)
#   If PnL <= 0, it just reports and does nothing else.
# - Safety checks:
#       • Won't transfer below MAIN_BAL_FLOOR_USD
#       • Skips tiny amounts and respects DRIP_MIN_USD for SUBS part
# - Idempotent:
#       • Stores the last swept London-date to avoid double runs.
#       • Stores a persistent subaccount round-robin index.
#
# Enhancements vs previous version:
#   1) Uses central notifier (get_notifier("main")) instead of raw send_tg.
#   2) Fixes SUBS distribution bug (no more over-sending) and persists RR index.
#   3) Adds cursor pagination for closed PnL (handles >200 entries/day).
#   4) Safer Decimal handling around MAIN_BAL_FLOOR_USD and DRIP_MIN_USD.

import time
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pytz
import orjson

from app.core.flashback_common import (
    bybit_get,
    bybit_post,
    get_equity_usdt,
    inter_transfer_usdt_to_sub,
    SUB_UIDS_ROUND_ROBIN,
    SWEEP_CUTOFF_TZ,
    SWEEP_CUTOFF_HHMM,
    SWEEP_ALLOCATION,
    MAIN_BAL_FLOOR_USD,
    DRIP_MIN_USD,
)

from app.core.notifier_bot import get_notifier

tg = get_notifier("main")

STATE_PATH = Path("app/state/profit_sweeper_state.json")
CATEGORY = "linear"
POLL_SECONDS = 30       # check time window every 30s
PAGE_LIMIT = 200        # closed PnL rows per page (we paginate now)

# --- Helpers for time & state ---


def _load_state() -> dict:
    """
    Load sweeper state from disk.
    Keys:
      - last_swept_date: "YYYY-MM-DD" or None
      - sub_rr_index: int (next starting index in SUB_UIDS_ROUND_ROBIN)
    """
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {
        "last_swept_date": None,
        "sub_rr_index": 0,
    }


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(state))


def _london_now() -> datetime:
    tz = pytz.timezone(SWEEP_CUTOFF_TZ)
    return datetime.now(tz)


def _london_day_bounds(d: datetime) -> Tuple[int, int]:
    """
    Start/end of the current London day in ms epoch.
    """
    tz = pytz.timezone(SWEEP_CUTOFF_TZ)
    start = tz.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _is_cutoff_window(now: datetime) -> bool:
    """
    True when local London time is >= cutoff HH:MM and < cutoff + 2 minutes.
    This gives us a small window to fire once per day.
    """
    hh, mm = [int(x) for x in SWEEP_CUTOFF_HHMM.split(":")]
    cutoff = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    return (now >= cutoff) and (now < cutoff + timedelta(minutes=2))


def _fmt_usd(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


# --- PnL aggregation ---


def _sum_realized_pnl_today() -> Decimal:
    """
    Sum closed PnL in USDT for today's London session across linear category.
    Uses cursor pagination to handle >200 rows.
    """
    now_ldn = _london_now()
    start_ms, end_ms = _london_day_bounds(now_ldn)

    total = Decimal("0")
    cursor: Optional[str] = None
    pages = 0

    try:
        while True:
            params: Dict[str, str] = {
                "category": CATEGORY,
                "limit": str(PAGE_LIMIT),
                "startTime": str(start_ms),
                "endTime": str(end_ms),
            }
            if cursor:
                params["cursor"] = cursor

            r = bybit_get("/v5/position/closed-pnl", params)
            result = r.get("result", {}) or {}
            rows = result.get("list", []) or []

            for row in rows:
                try:
                    pnl = Decimal(str(row.get("closedPnl", "0") or "0"))
                except Exception:
                    pnl = Decimal("0")
                total += pnl

            cursor = result.get("nextPageCursor")
            pages += 1
            if not cursor or pages > 20:
                # safety cap: no more than 20*200 = 4000 rows/day
                break

    except Exception as e:
        tg.error(f"[Sweeper] closed-pnl fetch error: {e}")

    return total


# --- Transfers ---


def _transfer_unified_to_funding_usdt(amount: Decimal) -> bool:
    """
    Universal transfer UNIFIED -> FUNDING for the main account.
    """
    if amount <= 0:
        return False
    body = {
        "transferId": str(int(time.time() * 1000)),
        "coin": "USDT",
        "amount": str(amount),
        "fromAccountType": "UNIFIED",
        "toAccountType": "FUNDING",
    }
    try:
        bybit_post("/v5/asset/transfer/universal-transfer", body)
        return True
    except Exception as e:
        tg.error(f"[Sweeper] Funding transfer failed: {e}")
        return False


# --- Allocation parsing ---


def _parse_allocation(spec: str) -> List[Tuple[Decimal, str]]:
    """
    "60:MAIN,25:FUNDING,15:SUBS" -> [(60, 'MAIN'), (25, 'FUNDING'), (15, 'SUBS')]
    """
    parts: List[Tuple[Decimal, str]] = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        pct_str, dest = item.split(":")
        parts.append((Decimal(pct_str), dest.strip().upper()))
    tot = sum(p for p, _ in parts)
    if tot != Decimal("100"):
        tg.warn(f"[Sweeper] Allocation totals {tot}%, not 100%. Proceeding anyway.")
    return parts


# --- Core sweep ---


def _sweep_once(today_str: str, state: dict) -> None:
    """
    Execute a single daily sweep if profitable.
    Updates state["sub_rr_index"] for round-robin SUBS distribution.
    """
    realized = _sum_realized_pnl_today()

    # Report even if <= 0
    if realized <= 0:
        tg.info(f"📉 Daily PnL (London {today_str}): {_fmt_usd(realized)} — no sweep.")
        return

    # Equity check and floor
    equity = get_equity_usdt()
    allocs = _parse_allocation(SWEEP_ALLOCATION)

    # Normalize floor & drip to Decimals
    floor = Decimal(str(MAIN_BAL_FLOOR_USD))
    drip_min = Decimal(str(DRIP_MIN_USD))

    legs: List[Tuple[str, Decimal]] = []
    for pct, dest in allocs:
        amt = (realized * pct / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        legs.append((dest, amt))

    # Total intended outgoing (FUNDING + SUBS)
    outgoing_total = sum(amt for dest, amt in legs if dest in ("FUNDING", "SUBS"))

    # If outgoing would push equity below floor, trim SUBS+FUNDING legs
    if (equity - outgoing_total) < floor:
        need_cut = floor - (equity - outgoing_total)
        if need_cut > 0:
            adjusted: List[Tuple[str, Decimal]] = []
            for dest, amt in legs:
                if dest in ("SUBS", "FUNDING") and need_cut > 0 and amt > 0:
                    cut = min(amt, need_cut)
                    amt -= cut
                    need_cut -= cut
                adjusted.append((dest, amt))
            legs = adjusted

    subs = [x for x in SUB_UIDS_ROUND_ROBIN.split(",") if x.strip()]
    sub_count = len(subs)
    sub_rr_index = int(state.get("sub_rr_index", 0)) if sub_count > 0 else 0

    details: List[str] = []

    # Perform transfers
    for dest, amt in legs:
        if amt <= 0:
            details.append(f"{dest}: {_fmt_usd(Decimal('0'))}")
            continue

        if dest == "MAIN":
            # No transfer; it stays
            details.append(f"MAIN: {_fmt_usd(amt)}")

        elif dest == "FUNDING":
            ok = _transfer_unified_to_funding_usdt(amt)
            details.append(f"FUNDING: {_fmt_usd(amt)}{' ✅' if ok else ' ❌'}")

        elif dest == "SUBS":
            if not subs:
                details.append("SUBS: 0 (no sub UIDs configured)")
                continue

            remaining = amt
            sent_total = Decimal("0")
            used_slots = 0

            # Equal-ish distribution with round-robin, respecting DRIP_MIN_USD per sub
            for slot in range(sub_count):
                if remaining < drip_min:
                    break

                slots_left = sub_count - slot
                part = (remaining / slots_left).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if part < drip_min:
                    break

                uid = subs[(sub_rr_index + slot) % sub_count]
                try:
                    inter_transfer_usdt_to_sub(uid, part)
                    sent_total += part
                    remaining -= part
                    used_slots += 1
                except Exception as e:
                    tg.warn(f"[Sweeper] Sub transfer to {uid} failed: {e}")

            # Advance the round-robin index by how many subs we actually used
            if sub_count > 0 and used_slots > 0:
                sub_rr_index = (sub_rr_index + used_slots) % sub_count

            details.append(f"SUBS: {_fmt_usd(sent_total)}")

        else:
            details.append(f"{dest}: {_fmt_usd(amt)} (unknown dest; skipped)")

    # Persist updated RR index
    if sub_count > 0:
        state["sub_rr_index"] = sub_rr_index

    msg = (
        f"✅ Profit Sweep (London {today_str})\n"
        f"Realized: {_fmt_usd(realized)}\n"
        f"Equity:  {_fmt_usd(equity)}\n"
        + "\n".join(f"• {d}" for d in details)
    )
    tg.info(msg)


def loop():
    tg.info("🧾 Flashback Profit Sweeper started.")
    state = _load_state()

    while True:
        try:
            now = _london_now()
            today_str = now.strftime("%Y-%m-%d")

            if _is_cutoff_window(now):
                if state.get("last_swept_date") != today_str:
                    _sweep_once(today_str, state)
                    state["last_swept_date"] = today_str
                    _save_state(state)
                # Sleep past the window to avoid duplicate in the same minute
                time.sleep(90)
            else:
                time.sleep(POLL_SECONDS)

        except Exception as e:
            tg.error(f"[Sweeper] {e}")
            time.sleep(10)


if __name__ == "__main__":
    loop()
