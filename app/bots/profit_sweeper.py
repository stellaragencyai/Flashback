# app/bots/profit_sweeper.py
# Flashback — Profit Sweeper (Main, hardened + DRY_RUN + dedicated TG + console + force-run)
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
# Extra hardening:
#   - ROOT-relative state path: state/profit_sweeper_state.json (no more CWD dependency).
#   - SWEEPER_DRY_RUN env flag:
#         SWEEPER_DRY_RUN=true  -> simulate all transfers, log only.
#         SWEEPER_DRY_RUN=false -> perform real Bybit transfers (original behavior).
#   - Dedicated Telegram channel: "profit_sweeper" (separate bot/token from main).
#   - Console + Telegram logging (so you see everything in the terminal).
#   - SWEEP_FORCE_RUN=true -> run once immediately for today's London date, then exit.
#
# Env of interest:
#   SWEEP_CUTOFF_TZ      (e.g., Europe/London)
#   SWEEP_CUTOFF_HHMM    (e.g., 23:59)
#   SWEEP_ALLOCATION     (e.g., "75:MAIN,10:FUNDING,15:SUBS")
#   MAIN_BAL_FLOOR_USD
#   DRIP_MIN_USD
#   SWEEPER_DRY_RUN      ("true"/"false")
#   SWEEP_FORCE_RUN      ("true"/"false")

import os
import time
import traceback
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pytz

# --- Constants ---
PAGE_LIMIT = 200
CATEGORY = "linear"  # You may want to adjust this if needed
POLL_SECONDS = 30  # Polling interval in seconds when not in cutoff window

# orjson compatibility wrapper: prefer orjson if installed, otherwise fall back to stdlib json.
try:
    import orjson as _orjson  # type: ignore

    def orjson_loads(b):
        return _orjson.loads(b)

    def orjson_dumps(obj):
        return _orjson.dumps(obj)

except Exception:
    import json as _json

    def orjson_loads(b):
        # accept bytes or str like orjson
        if isinstance(b, (bytes, bytearray)):
            b = b.decode()
        return _json.loads(b)

    def orjson_dumps(obj):
        # return bytes to mimic orjson.dumps behaviour
        return _json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode()

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

# Use dedicated Telegram channel "profit_sweeper"
# .env should define:
#   TG_TOKEN_PROFIT_SWEEPER=...
#   TG_CHAT_PROFIT_SWEEPER=...
tg = get_notifier("profit_sweeper")

# --- Paths & DRY_RUN config ---

# ROOT_DIR = project root: .../Flashback
ROOT_DIR = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "profit_sweeper_state.json"


def _parse_bool(val, default=False):
    """
    Parse a boolean from environment variable strings.
    Accepts: "1", "true", "yes", "on" (case-insensitive) as True.
    """
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "on")


SWEEPER_DRY_RUN = _parse_bool(os.getenv("SWEEPER_DRY_RUN"), True)
SWEEP_FORCE_RUN = _parse_bool(os.getenv("SWEEP_FORCE_RUN"), False)


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


def _load_state() -> dict:
    """
    Load sweeper state from disk.
    Keys:
      - last_swept_date: "YYYY-MM-DD" or None
      - sub_rr_index: int (next starting index in SUB_UIDS_ROUND_ROBIN)
    """
    try:
        if STATE_PATH.exists():
            return orjson_loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {
        "last_swept_date": None,
        "sub_rr_index": 0,
    }


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson_dumps(state))


def _get_tz():
    try:
        return pytz.timezone(SWEEP_CUTOFF_TZ)
    except Exception:
        log_warn(f"[Sweeper] Invalid SWEEP_CUTOFF_TZ={SWEEP_CUTOFF_TZ!r}, falling back to UTC.")
        return pytz.UTC


def _london_now() -> datetime:
    tz = _get_tz()
    return datetime.now(tz)


def _london_day_bounds(d: datetime) -> Tuple[int, int]:
    """
    Start/end of the current London day in ms epoch.
    """
    tz = _get_tz()
    start = tz.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _is_cutoff_window(now: datetime) -> bool:
    """
    True when local London time is >= cutoff HH:MM and < cutoff + 2 minutes.
    This gives us a small window to fire once per day.
    """
    try:
        hh, mm = [int(x) for x in SWEEP_CUTOFF_HHMM.split(":")]
    except Exception:
        log_warn(f"[Sweeper] Invalid SWEEP_CUTOFF_HHMM={SWEEP_CUTOFF_HHMM!r}, using 23:59 fallback.")
        hh, mm = 23, 59

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
        log_error(f"[Sweeper] closed-pnl fetch error: {e}")

    return total


# --- Transfers ---


def _transfer_unified_to_funding_usdt(amount: Decimal) -> bool:
    """
    Universal transfer UNIFIED -> FUNDING for the main account.
    Honors SWEEPER_DRY_RUN to avoid accidental live moves.
    """
    if amount <= 0:
        return False

    if SWEEPER_DRY_RUN:
        log_info(f"[Sweeper] DRY_RUN: would transfer {_fmt_usd(amount)} from UNIFIED -> FUNDING.")
        return True

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
        log_error(f"[Sweeper] Funding transfer failed: {e}")
        return False


# --- Allocation parsing ---


def _parse_allocation(spec: str) -> List[Tuple[Decimal, str]]:
    """
    "60:MAIN,25:FUNDING,15:SUBS" -> [(60, 'MAIN'), (25, 'FUNDING'), (15, 'SUBS')]

    Robust:
      - Ignores malformed chunks (no colon, bad decimal).
      - If nothing valid, falls back to 75:MAIN,10:FUNDING,15:SUBS.
    """
    parts: List[Tuple[Decimal, str]] = []
    for raw in spec.split(","):
        item = raw.strip()
        if not item:
            continue
        if ":" not in item:
            log_warn(f"[Sweeper] Bad allocation item (no colon): {item!r}, skipping.")
            continue
        pct_str, dest = item.split(":", 1)
        try:
            pct = Decimal(pct_str)
        except Exception:
            log_warn(f"[Sweeper] Bad allocation percent {pct_str!r} in {item!r}, skipping.")
            continue
        dest = dest.strip().upper()
        if not dest:
            log_warn(f"[Sweeper] Empty destination in {item!r}, skipping.")
            continue
        parts.append((pct, dest))

    # Fallback if everything was garbage
    if not parts:
        log_error(
            f"[Sweeper] SWEEP_ALLOCATION={spec!r} produced no valid parts; "
            "using fallback 75:MAIN,10:FUNDING,15:SUBS."
        )
        parts = [
            (Decimal("75"), "MAIN"),
            (Decimal("10"), "FUNDING"),
            (Decimal("15"), "SUBS"),
        ]

    tot = sum(p for p, _ in parts)
    if tot != Decimal("100"):
        log_warn(f"[Sweeper] Allocation totals {tot}%, not 100%. Proceeding anyway.")
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
        log_info(f"📉 Daily PnL (London {today_str}): {_fmt_usd(realized)} — no sweep.")
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

    equity_dec = Decimal(str(equity))

    # If outgoing would push equity below floor, trim SUBS+FUNDING legs
    if (equity_dec - outgoing_total) < floor:
        need_cut = floor - (equity_dec - outgoing_total)
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
            suffix = " (DRY_RUN)" if SWEEPER_DRY_RUN else ""
            details.append(f"FUNDING: {_fmt_usd(amt)}{' ✅' if ok else ' ❌'}{suffix}")

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
                    if SWEEPER_DRY_RUN:
                        log_info(
                            f"[Sweeper] DRY_RUN: would transfer {_fmt_usd(part)} from UNIFIED -> sub UID {uid}."
                        )
                    else:
                        inter_transfer_usdt_to_sub(uid, part)

                    sent_total += part
                    remaining -= part
                    used_slots += 1
                except Exception as e:
                    log_warn(f"[Sweeper] Sub transfer to {uid} failed: {e}")

            # Advance the round-robin index by how many subs we actually used
            if sub_count > 0 and used_slots > 0:
                sub_rr_index = (sub_rr_index + used_slots) % sub_count

            suffix = " (DRY_RUN)" if SWEEPER_DRY_RUN else ""
            details.append(f"SUBS: {_fmt_usd(sent_total)}{suffix}")

        else:
            details.append(f"{dest}: {_fmt_usd(amt)} (unknown dest; skipped)")

    # Persist updated RR index
    if sub_count > 0:
        state["sub_rr_index"] = sub_rr_index

    msg = (
        f"✅ Profit Sweep (London {today_str})\n"
        f"Realized: {_fmt_usd(realized)}\n"
        f"Equity:  {_fmt_usd(equity_dec)}\n"
        + "\n".join(f"• {d}" for d in details)
        + f"\n\nSWEEPER_DRY_RUN: {'ON' if SWEEPER_DRY_RUN else 'OFF'}"
    )
    log_info(msg)


def loop():
    log_info(
        "🧾 Flashback Profit Sweeper started (via supervisor).\n"
        f"SWEEPER_DRY_RUN: {'ON' if SWEEPER_DRY_RUN else 'OFF'}\n"
        f"SWEEP_FORCE_RUN: {'ON' if SWEEP_FORCE_RUN else 'OFF'}\n"
        f"State file: {STATE_PATH}"
    )
    state = _load_state()

    # One-shot mode for testing: run a sweep immediately, then exit
    if SWEEP_FORCE_RUN:
        now = _london_now()
        today_str = now.strftime("%Y-%m-%d")
        _sweep_once(today_str, state)
        state["last_swept_date"] = today_str
        _save_state(state)
        log_info("[Sweeper] Force run completed; exiting because SWEEP_FORCE_RUN=true.")
        return

    # Normal daemon mode: wait for cutoff window each day
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
            tb = traceback.format_exc()
            log_error(f"[Sweeper] Unhandled error: {e}\n{tb}")
            time.sleep(10)


if __name__ == "__main__":
    loop()
