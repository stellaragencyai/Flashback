# app/bots/profit_sweeper.py
# Flashback — Profit Sweeper (Main, hardened + DRY_RUN + dedicated TG + console + multi-sweep)
#
# What it does
# - Up to 3 times per London day, ~8 hours apart:
#       • Computes *incremental* realized PnL (USDT) on the main account
#         since the last sweep, bounded to the current London day.
#       • If incremental PnL > 0 and >= SWEEP_MIN_PNL_USD:
#             Allocate per SWEEP_ALLOCATION (e.g., "80:MAIN,20:SUBS").
#             · MAIN     -> no transfer; remains in main (UNIFIED)
#             · FUNDING  -> UNIFIED -> FUNDING internal transfer
#             · SUBS     -> round-robin UNIFIED -> sub UNIFIED (by MemberId UID list)
#         If incremental PnL <= 0 or below SWEEP_MIN_PNL_USD, it just reports and does nothing else.
#
# - Safety checks:
#       • Won't transfer below MAIN_BAL_FLOOR_USD
#       • Skips tiny amounts and respects DRIP_MIN_USD for SUBS part
#
# - Idempotent / stateful:
#       • Stores:
#             - last_swept_date (London "YYYY-MM-DD")
#             - last_sweep_ms (epoch ms of last sweep)
#             - sweeps_today (0–3 per London date)
#             - sub_rr_index (round-robin index for sub UIDs)
#
# Extra hardening:
#   - ROOT-relative state path: state/profit_sweeper_state.json (no more CWD dependency).
#   - SWEEPER_DRY_RUN env flag:
#         SWEEPER_DRY_RUN=true  -> simulate all transfers, log only.
#         SWEEPER_DRY_RUN=false -> perform real Bybit transfers (original behavior).
#   - Dedicated Telegram channel: "profit_sweeper" (separate bot/token from main).
#   - Console + Telegram logging (so you see everything in the terminal).
#   - SWEEP_FORCE_RUN=true -> run once immediately using the incremental PnL
#       since the last sweep boundary (or day start), then exit.
#
# Env of interest:
#   SWEEP_CUTOFF_TZ      (e.g., Europe/London)  [used for "day" boundaries]
#   SWEEP_ALLOCATION     (e.g., "80:MAIN,20:SUBS")
#   MAIN_BAL_FLOOR_USD
#   DRIP_MIN_USD
#   SWEEPER_DRY_RUN      ("true"/"false")
#   SWEEP_FORCE_RUN      ("true"/"false")
#   SWEEP_MIN_PNL_USD    (minimum incremental PnL to trigger a sweep; default 0 = no threshold)

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
POLL_SECONDS = 30  # Polling interval in seconds when not sweeping
SWEEP_INTERVAL_SECONDS = 8 * 60 * 60  # 8 hours between sweeps

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

# Minimum incremental PnL threshold (USD) to trigger a sweep
try:
    SWEEP_MIN_PNL_USD = Decimal(str(os.getenv("SWEEP_MIN_PNL_USD", "0")))
except Exception:
    SWEEP_MIN_PNL_USD = Decimal("0")


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
      - last_swept_date: "YYYY-MM-DD" or None (London date)
      - last_sweep_ms: epoch ms of last sweep (or None)
      - sweeps_today: int (0..3)
      - sub_rr_index: int (next starting index in SUB_UIDS_ROUND_ROBIN)
    """
    try:
        if STATE_PATH.exists():
            data = orjson_loads(STATE_PATH.read_bytes())
        else:
            data = {}
    except Exception:
        data = {}

    return {
        "last_swept_date": data.get("last_swept_date"),
        "last_sweep_ms": data.get("last_sweep_ms"),
        "sweeps_today": int(data.get("sweeps_today", 0) or 0),
        "sub_rr_index": int(data.get("sub_rr_index", 0) or 0),
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


def _fmt_usd(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"


# --- PnL aggregation ---


def _sum_realized_pnl_interval(start_ms: int, end_ms: int) -> Decimal:
    """
    Sum closed PnL in USDT for [start_ms, end_ms] across linear category.
    Uses cursor pagination to handle >200 rows.
    """
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
                # safety cap: no more than 20*200 = 4000 rows/interval
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
    "80:MAIN,20:SUBS" -> [(80, 'MAIN'), (20, 'SUBS')]

    Robust:
      - Ignores malformed chunks (no colon, bad decimal).
      - If nothing valid, falls back to 80:MAIN,20:SUBS.
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
            "using fallback 80:MAIN,20:SUBS."
        )
        parts = [
            (Decimal("80"), "MAIN"),
            (Decimal("20"), "SUBS"),
        ]

    tot = sum(p for p, _ in parts)
    if tot != Decimal("100"):
        log_warn(f"[Sweeper] Allocation totals {tot}%, not 100%. Proceeding anyway.")
    return parts


# --- Core sweep ---


def _sweep_once(now: datetime, state: dict, label: str = "") -> None:
    """
    Execute a single sweep if incremental PnL is positive and above SWEEP_MIN_PNL_USD.
    Uses PnL from max(day_start, last_sweep_ms) .. now_ms.
    Updates state["sub_rr_index"] for round-robin SUBS distribution.
    """
    today_str = now.strftime("%Y-%m-%d")
    now_ms = int(now.timestamp() * 1000)
    day_start_ms, day_end_ms = _london_day_bounds(now)

    # Ensure interval is within today's London session
    last_sweep_ms_raw = state.get("last_sweep_ms")
    try:
        last_sweep_ms = int(last_sweep_ms_raw) if last_sweep_ms_raw is not None else None
    except Exception:
        last_sweep_ms = None

    if last_sweep_ms is None or last_sweep_ms < day_start_ms:
        start_ms = day_start_ms
    else:
        start_ms = last_sweep_ms

    # Clip end to not exceed day_end_ms (paranoia)
    end_ms = min(now_ms, day_end_ms)

    if end_ms <= start_ms:
        log_info(
            f"📉 No valid interval for sweep (London {today_str}) "
            f"[start_ms={start_ms}, end_ms={end_ms}] — skipping."
        )
        return

    realized = _sum_realized_pnl_interval(start_ms, end_ms)

    # Report even if <= 0 or below threshold
    if realized <= 0:
        log_info(
            f"📉 Incremental PnL (London {today_str} {label or ''} "
            f"{start_ms}->{end_ms}): {_fmt_usd(realized)} — no sweep."
        )
        return

    if realized < SWEEP_MIN_PNL_USD:
        log_info(
            f"📉 Incremental PnL (London {today_str} {label or ''} "
            f"{start_ms}->{end_ms}): {_fmt_usd(realized)} < "
            f"min threshold {_fmt_usd(SWEEP_MIN_PNL_USD)} — no sweep."
        )
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
                            f"[Sweeper] DRY_RUN: would transfer "
                            f"{_fmt_usd(part)} from UNIFIED -> sub UID {uid}."
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

    # Persist updated RR index & last_sweep_ms
    if sub_count > 0:
        state["sub_rr_index"] = sub_rr_index
    state["last_sweep_ms"] = end_ms

    msg = (
        f"✅ Profit Sweep (London {today_str}{' ' + label if label else ''})\n"
        f"Incremental PnL: {_fmt_usd(realized)}\n"
        f"Equity:          {_fmt_usd(equity_dec)}\n"
        + "\n".join(f"• {d}" for d in details)
        + f"\n\nSWEEPER_DRY_RUN: {'ON' if SWEEPER_DRY_RUN else 'OFF'}\n"
        + f"SWEEP_MIN_PNL_USD: {_fmt_usd(SWEEP_MIN_PNL_USD)}"
    )
    log_info(msg)


def loop():
    log_info(
        "🧾 Flashback Profit Sweeper started (via supervisor).\n"
        f"SWEEPER_DRY_RUN: {'ON' if SWEEPER_DRY_RUN else 'OFF'}\n"
        f"SWEEP_FORCE_RUN: {'ON' if SWEEP_FORCE_RUN else 'OFF'}\n"
        f"SWEEP_MIN_PNL_USD: {_fmt_usd(SWEEP_MIN_PNL_USD)}\n"
        f"State file: {STATE_PATH}\n"
        f"Schedule: up to 3 sweeps per London day, >= {SWEEP_INTERVAL_SECONDS / 3600:.0f}h apart."
    )
    state = _load_state()

    # One-shot mode for testing: run a sweep immediately, then exit
    if SWEEP_FORCE_RUN:
        now = _london_now()
        today_str = now.strftime("%Y-%m-%d")
        _sweep_once(now, state, label="FORCE")
        # Update daily stats as if this counted as a sweep
        state["last_swept_date"] = today_str
        state["sweeps_today"] = int(state.get("sweeps_today", 0) or 0) + 1
        _save_state(state)
        log_info("[Sweeper] Force run completed; exiting because SWEEP_FORCE_RUN=true.")
        return

    # Normal daemon mode
    while True:
        try:
            now = _london_now()
            today_str = now.strftime("%Y-%m-%d")
            now_ms = int(now.timestamp() * 1000)

            last_date = state.get("last_swept_date")
            sweeps_today = int(state.get("sweeps_today", 0) or 0)

            # New London day -> reset sweep counter
            if last_date != today_str:
                state["last_swept_date"] = today_str
                sweeps_today = 0
                state["sweeps_today"] = sweeps_today
                # Do not reset last_sweep_ms; _sweep_once handles day boundaries

            if sweeps_today < 3:
                last_sweep_ms_raw = state.get("last_sweep_ms")
                try:
                    last_sweep_ms = int(last_sweep_ms_raw) if last_sweep_ms_raw is not None else None
                except Exception:
                    last_sweep_ms = None

                should_sweep = False
                if last_sweep_ms is None:
                    # Never swept before -> allow immediately
                    should_sweep = True
                else:
                    delta = (now_ms - last_sweep_ms) / 1000.0
                    if delta >= SWEEP_INTERVAL_SECONDS:
                        should_sweep = True

                if should_sweep:
                    label = f"run #{sweeps_today + 1}"
                    _sweep_once(now, state, label=label)
                    sweeps_today += 1
                    state["sweeps_today"] = sweeps_today
                    _save_state(state)
                    # Avoid hammering immediately again
                    time.sleep(60)
                else:
                    time.sleep(POLL_SECONDS)
            else:
                # Already hit 3 sweeps today, just chill until next day
                time.sleep(POLL_SECONDS)

        except Exception as e:
            tb = traceback.format_exc()
            log_error(f"[Sweeper] Unhandled error: {e}\n{tb}")
            time.sleep(10)


if __name__ == "__main__":
    loop()
