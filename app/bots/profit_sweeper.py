# app/bots/profit_sweeper.py
# Flashback — Profit Sweeper (Main)
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
#       • Skips tiny amounts (< $0.01 effective) and respects DRIP_MIN_USD semantics for SUBS parts
# - Idempotent: stores the last swept London-date to avoid double runs if left on.
#
# Requirements
# - .env keys set, including SWEEP_CUTOFF_TZ, SWEEP_CUTOFF_HHMM, SUB_UIDS_ROUND_ROBIN
# - flashback_common helpers present
# - pytz installed

import time
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pytz
import orjson

from app.core.flashback_common import (
    bybit_get, bybit_post, send_tg, get_equity_usdt,
    inter_transfer_usdt_to_sub, SUB_UIDS_ROUND_ROBIN,
    SWEEP_CUTOFF_TZ, SWEEP_CUTOFF_HHMM, SWEEP_ALLOCATION,
    MAIN_BAL_FLOOR_USD, DRIP_MIN_USD
)

STATE_PATH = Path("app/state/profit_sweeper_state.json")
CATEGORY = "linear"
POLL_SECONDS = 30       # check time window every 30s
PAGE_LIMIT = 200        # pull up to 200 closed PnL rows per call

# --- Helpers for time & state ---

def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {"last_swept_date": None}

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
    We page once (200 rows). If you close >200 positions/day, we can extend to cursor pagination later.
    """
    now_ldn = _london_now()
    start_ms, end_ms = _london_day_bounds(now_ldn)
    # Bybit v5 closed-pnl has startTime/endTime; returns per symbol rows
    total = Decimal("0")
    try:
        r = bybit_get("/v5/position/closed-pnl", {
            "category": CATEGORY,
            "limit": str(PAGE_LIMIT),
            "startTime": str(start_ms),
            "endTime": str(end_ms),
        })
        rows = r.get("result", {}).get("list", []) or []
        for row in rows:
            # closedPnl is in coin; on linear USDT it’s USDT
            try:
                pnl = Decimal(str(row.get("closedPnl", "0") or "0"))
            except Exception:
                pnl = Decimal("0")
            total += pnl
    except Exception as e:
        send_tg(f"[Sweeper] closed-pnl fetch error: {e}")
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
        send_tg(f"[Sweeper] Funding transfer failed: {e}")
        return False

# --- Allocation parsing ---

def _parse_allocation(spec: str) -> List[Tuple[Decimal, str]]:
    """
    "60:MAIN,25:FUNDING,15:SUBS" -> [(60, 'MAIN'), (25, 'FUNDING'), (15, 'SUBS')]
    """
    parts = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        pct, dest = item.split(":")
        parts.append((Decimal(pct), dest.strip().upper()))
    # sanity: total roughly 100
    tot = sum(p for p, _ in parts)
    if tot != Decimal("100"):
        send_tg(f"[Sweeper] Allocation totals {tot}%, not 100%. Proceeding anyway.")
    return parts

# --- Core sweep ---

def _sweep_once(today_str: str) -> None:
    """
    Execute a single daily sweep if profitable.
    """
    realized = _sum_realized_pnl_today()

    # Report even if <= 0
    if realized <= 0:
        send_tg(f"📉 Daily PnL (London {today_str}): {_fmt_usd(realized)} — no sweep.")
        return

    # Equity check and floor
    equity = get_equity_usdt()
    allocs = _parse_allocation(SWEEP_ALLOCATION)
    details = []
    remaining = realized

    # Compute each leg's amount (by Pct of 'realized')
    legs: List[Tuple[str, Decimal]] = []
    for pct, dest in allocs:
        amt = (realized * pct / Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        legs.append((dest, amt))

    # Execute legs
    subs = [x for x in SUB_UIDS_ROUND_ROBIN.split(",") if x.strip()]
    sub_rr = 0

    # Always ensure we leave the main balance above floor after any outgoing transfers (FUNDING + SUBS)
    outgoing_total = sum(amt for dest, amt in legs if dest in ("FUNDING", "SUBS"))
    if (equity - outgoing_total) < Decimal(MAIN_BAL_FLOOR_USD):
        # Reduce outgoing pro-rata to respect floor
        over = Decimal(MAIN_BAL_FLOOR_USD) - (equity - outgoing_total)
        # If we can’t satisfy floor, cut SUBS first, then FUNDING
        adj = []
        need_cut = over
        for dest, amt in legs:
            if dest in ("SUBS", "FUNDING") and need_cut > 0 and amt > 0:
                cut = min(amt, need_cut)
                amt -= cut
                need_cut -= cut
            adj.append((dest, amt))
        legs = adj

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
            # Round-robin across subs with DRIP_MIN_USD floor
            sent_total = Decimal("0")
            dv = amt
            i = 0
            while dv >= Decimal(DRIP_MIN_USD) and i < len(subs):
                part = (dv / (len(subs) - i)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if part < Decimal(DRIP_MIN_USD):
                    break
                uid = subs[(sub_rr + i) % len(subs)]
                try:
                    inter_transfer_usdt_to_sub(uid, part)
                    sent_total += part
                except Exception as e:
                    send_tg(f"[Sweeper] Sub transfer to {uid} failed: {e}")
                i += 1
            details.append(f"SUBS: {_fmt_usd(sent_total)}")
        else:
            details.append(f"{dest}: {_fmt_usd(amt)} (unknown dest; skipped)")

    msg = (
        f"✅ Profit Sweep (London {today_str})\n"
        f"Realized: {_fmt_usd(realized)}\n"
        + "\n".join(f"• {d}" for d in details)
    )
    send_tg(msg)

def loop():
    send_tg("🧾 Flashback Profit Sweeper started.")
    state = _load_state()

    while True:
        try:
            now = _london_now()
            today_str = now.strftime("%Y-%m-%d")

            if _is_cutoff_window(now):
                if state.get("last_swept_date") != today_str:
                    _sweep_once(today_str)
                    state["last_swept_date"] = today_str
                    _save_state(state)
                # Sleep past the window to avoid duplicate in the same minute
                time.sleep(90)
            else:
                time.sleep(POLL_SECONDS)

        except Exception as e:
            send_tg(f"[Sweeper] {e}")
            time.sleep(10)

if __name__ == "__main__":
    loop()
