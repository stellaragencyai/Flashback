#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — CarRush Mission Daemon

Purpose
-------
Track the ultra-aggressive "CarRush" subaccount that tries to go from 25 -> 25,000
in 10 days, and expose a simple mission state for other bots:

    state/car_mission_state.json

Key responsibilities:
- Poll Bybit for USDT equity on the mission subaccount.
- Map equity -> Level (L1..L10) based on configured thresholds (25,50,...,25000).
- Map Level -> Tier:
      Tier A: Levels 1-3  (1 open trade max)
      Tier B: Levels 4-6  (2 open trades max)
      Tier C: Levels 7-10 (3 open trades max)
- Track:
      • mission_start_ts
      • hours_elapsed / hours_remaining (based on CAR_MISSION_DURATION_HOURS)
      • peak_equity and max_drawdown_pct
- Detect:
      • Level ups  (progress only, does NOT end session)
      • Tier ups   (SESSION GOAL HIT: end-of-session trigger)
      • Mission completion (equity >= last level threshold)
- Goals (simplified):
      • session_goal: "NEXT_TIER" (default) or "NONE"
        -> when tier changes upward, session_goal_hit = true
           (use this in guards to block new trades until you manually reset / new day)
- Daily reset:
      • At each new day in CAR_MISSION_TZ, reset session_goal_hit
        and record new "today" date.

Configuration (.env)
--------------------
CAR_MISSION_SUB_UID         = 524700541          # subaccount UID (for reference / guard)
CAR_MISSION_LABEL           = flashback10        # human label, also notifier channel by default
CAR_MISSION_TG_CHANNEL      = flashback10        # notifier_bot channel name
CAR_MISSION_API_KEY         = <mission api key>  # Bybit unified trading key for this sub
CAR_MISSION_API_SECRET      = <mission secret>
CAR_MISSION_LEVELS          = 25,50,100,250,500,1000,2500,5000,10000,25000
CAR_MISSION_DURATION_HOURS  = 240                # 10 days by default
CAR_MISSION_TZ              = Europe/London
CAR_MISSION_POLL_SECONDS    = 5

It is safe to run this daemon continuously. If mission_* env values are missing,
it will log warnings and sleep.
"""

from __future__ import annotations

import json
import os
import time
import hmac
import hashlib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.core.logger import get_logger
from app.core.notifier_bot import get_notifier

log = get_logger("car_mission_daemon")

# ---------------------------------------------------------------------------
# Paths & env loading
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

ENV_PATH = ROOT / ".env"
if ENV_PATH.exists():
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(ENV_PATH)
        log.info("[CarMission] Loaded .env from %s", ENV_PATH)
    except Exception as e:  # pragma: no cover
        log.warning("[CarMission] Failed to load .env: %s", e)
else:
    log.warning("[CarMission] .env not found at %s; using OS env only.", ENV_PATH)

CAR_STATE_PATH = STATE_DIR / "car_mission_state.json"

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")
BYBIT_RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "20000")
ACCOUNT_TYPE = "UNIFIED"

CAR_MISSION_SUB_UID = os.getenv("CAR_MISSION_SUB_UID", "").strip()
CAR_MISSION_LABEL = os.getenv("CAR_MISSION_LABEL", "flashback10").strip()
CAR_MISSION_TG_CHANNEL = os.getenv("CAR_MISSION_TG_CHANNEL", CAR_MISSION_LABEL).strip()

CAR_MISSION_API_KEY = os.getenv("CAR_MISSION_API_KEY", "").strip()
CAR_MISSION_API_SECRET = os.getenv("CAR_MISSION_API_SECRET", "").strip()

LEVELS_STR = os.getenv(
    "CAR_MISSION_LEVELS",
    "25,50,100,250,500,1000,2500,5000,10000,25000",
).strip()

MISSION_DURATION_HOURS = float(os.getenv("CAR_MISSION_DURATION_HOURS", "240"))
CAR_MISSION_TZ = os.getenv("CAR_MISSION_TZ", "Europe/London").strip()
POLL_SECONDS = float(os.getenv("CAR_MISSION_POLL_SECONDS", "5"))

# Telegram notifier
tg = get_notifier(CAR_MISSION_TG_CHANNEL or "main")

# Timezone helper (standard library only)
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _now_local_date_str() -> str:
    from datetime import datetime, timezone

    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(CAR_MISSION_TZ)
        except Exception:
            tz = timezone.utc
    else:
        tz = timezone.utc
    return datetime.now(tz=tz).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Levels / tiers
# ---------------------------------------------------------------------------


@dataclass
class MissionLevels:
    levels: List[Decimal]

    @classmethod
    def from_env(cls) -> "MissionLevels":
        parts = [p.strip() for p in LEVELS_STR.split(",") if p.strip()]
        vals: List[Decimal] = []
        for p in parts:
            try:
                vals.append(Decimal(p))
            except Exception:
                continue
        if not vals:
            vals = [Decimal("25"), Decimal("50"), Decimal("100"), Decimal("250"),
                    Decimal("500"), Decimal("1000"), Decimal("2500"),
                    Decimal("5000"), Decimal("10000"), Decimal("25000")]
        vals = sorted(vals)
        return cls(levels=vals)

    def level_index_for_equity(self, eq: Decimal) -> int:
        """
        Return level index (0-based) for current equity.
        Example with default levels:
            eq < 25      -> 0
            25-<50       -> 0
            50-<100      -> 1
            ...
            >=25000      -> last index
        """
        if not self.levels:
            return 0
        idx = 0
        for i, thr in enumerate(self.levels):
            if eq >= thr:
                idx = i
            else:
                break
        return idx

    def label_for_index(self, idx: int) -> str:
        if not self.levels:
            return "0"
        idx = max(0, min(idx, len(self.levels) - 1))
        return str(self.levels[idx])

    def last_level_threshold(self) -> Decimal:
        if not self.levels:
            return Decimal("0")
        return self.levels[-1]


def tier_for_level_index(idx: int) -> Tuple[str, int]:
    """
    Map 0-based level index -> tier label + max concurrent trades.
    With default 10 levels:
        idx 0-2 -> Tier A -> 1 trade
        idx 3-5 -> Tier B -> 2 trades
        idx 6-9 -> Tier C -> 3 trades
    """
    if idx <= 2:
        return "A", 1
    elif idx <= 5:
        return "B", 2
    else:
        return "C", 3


# ---------------------------------------------------------------------------
# Bybit helpers
# ---------------------------------------------------------------------------


def _sign(timestamp: str, recv_window: str, query_string: str, body: str) -> str:
    payload = timestamp + CAR_MISSION_API_KEY + recv_window + query_string + body
    return hmac.new(
        CAR_MISSION_API_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def bybit_request(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: float = 8.0,
) -> Dict[str, Any]:
    if not CAR_MISSION_API_KEY or not CAR_MISSION_API_SECRET:
        raise RuntimeError("CAR_MISSION_API_KEY / _SECRET not configured.")

    url = BYBIT_BASE + path
    params = params or {}
    body = body or {}

    items = sorted((k, str(v)) for k, v in params.items())
    query_string = "&".join(f"{k}={v}" for k, v in items)

    body_str = json.dumps(body) if body else ""
    ts = str(int(time.time() * 1000))
    recv_window = BYBIT_RECV_WINDOW

    sign = _sign(ts, recv_window, query_string, body_str)

    headers = {
        "X-BAPI-API-KEY": CAR_MISSION_API_KEY,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json",
    }

    m = method.upper()
    if m == "GET":
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    else:
        resp = requests.post(url, params=params, data=body_str, headers=headers, timeout=timeout)

    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") not in (0, "0"):
        raise RuntimeError(f"Bybit error {data.get('retCode')}: {data.get('retMsg')}")
    return data


def get_equity_usdt() -> Decimal:
    try:
        data = bybit_request(
            "GET",
            "/v5/account/wallet-balance",
            {"accountType": ACCOUNT_TYPE, "coin": "USDT"},
        )
        lst = data.get("result", {}).get("list", []) or []
        if not lst:
            return Decimal("0")
        acct = lst[0]
        eq_str = acct.get("totalEquity") or acct.get("totalWalletBalance") or "0"
        return Decimal(str(eq_str))
    except Exception as e:
        log.warning("[CarMission] get_equity_usdt failed: %s", e)
        return Decimal("0")


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------


def load_state() -> Dict[str, Any]:
    if not CAR_STATE_PATH.exists():
        return {}
    try:
        raw = CAR_STATE_PATH.read_text(encoding="utf-8")
        return json.loads(raw)
    except Exception as e:
        log.warning("[CarMission] Failed to read state: %s", e)
        return {}


def save_state(state: Dict[str, Any]) -> None:
    try:
        tmp = CAR_STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, default=str, indent=2), encoding="utf-8")
        tmp.replace(CAR_STATE_PATH)
    except Exception as e:
        log.warning("[CarMission] Failed to save state: %s", e)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main_loop() -> None:
    levels_cfg = MissionLevels.from_env()

    log.info(
        "=== CarRush Mission Daemon === "
        "label=%s sub_uid=%s levels=%s duration_hours=%.1f tz=%s poll=%.1fs",
        CAR_MISSION_LABEL,
        CAR_MISSION_SUB_UID or "<unset>",
        [str(x) for x in levels_cfg.levels],
        MISSION_DURATION_HOURS,
        CAR_MISSION_TZ,
        POLL_SECONDS,
    )

    if not CAR_MISSION_API_KEY or not CAR_MISSION_API_SECRET:
        log.warning(
            "[CarMission] CAR_MISSION_API_KEY / _SECRET missing. "
            "Daemon will run but equity will always be 0."
        )

    if not CAR_MISSION_SUB_UID:
        log.warning("[CarMission] CAR_MISSION_SUB_UID is empty (used mainly for reference / guard).")

    tg.info(
        "✅ CarRush Mission Daemon ONLINE\n"
        f"Label: {CAR_MISSION_LABEL}\n"
        f"Sub UID: {CAR_MISSION_SUB_UID or 'n/a'}\n"
        f"Levels: {', '.join(str(x) for x in levels_cfg.levels)}\n"
        f"Duration: {MISSION_DURATION_HOURS:.0f}h"
    )

    state = load_state()

    # Initialize mission metadata
    now_ms = int(time.time() * 1000)
    if "mission_start_ts" not in state:
        state["mission_start_ts"] = now_ms
        state["mission_completed"] = False

    if "peak_equity" not in state:
        state["peak_equity"] = "0"
    if "max_drawdown_pct" not in state:
        state["max_drawdown_pct"] = 0.0

    if "last_level_index" not in state:
        state["last_level_index"] = 0
    if "last_tier" not in state:
        state["last_tier"] = "A"

    # SESSION GOAL = NEXT_TIER by default
    if "session_goal" not in state:
        state["session_goal"] = "NEXT_TIER"
    if "session_goal_hit" not in state:
        state["session_goal_hit"] = False

    if "last_day" not in state:
        state["last_day"] = _now_local_date_str()

    save_state(state)

    last_log_ts = 0.0

    while True:
        try:
            eq = get_equity_usdt()
            eq = eq.quantize(Decimal("0.01")) if eq > 0 else Decimal("0.00")

            # Mission timing
            now_ms = int(time.time() * 1000)
            mission_start_ts = int(state.get("mission_start_ts", now_ms))
            elapsed_hours = (now_ms - mission_start_ts) / 1000.0 / 3600.0
            hours_remaining = max(0.0, MISSION_DURATION_HOURS - elapsed_hours)

            # Daily reset
            today = _now_local_date_str()
            if today != state.get("last_day"):
                state["last_day"] = today
                state["session_goal_hit"] = False
                tg.info(f"🔄 New day detected ({today}) — session_goal_hit reset.")

            # Level & tier
            lvl_idx = levels_cfg.level_index_for_equity(eq)
            lvl_label = levels_cfg.label_for_index(lvl_idx)
            tier_label, tier_max_trades = tier_for_level_index(lvl_idx)

            prev_lvl_idx = int(state.get("last_level_index", 0))
            prev_tier_label = str(state.get("last_tier", "A"))

            # Peak equity & drawdown
            peak_equity = Decimal(str(state.get("peak_equity", "0")))
            if eq > peak_equity:
                peak_equity = eq
            dd_pct = 0.0
            if peak_equity > 0 and eq > 0:
                dd_pct = float((eq / peak_equity - Decimal("1.0")) * Decimal("100"))
            max_dd_pct = float(state.get("max_drawdown_pct", 0.0))
            if dd_pct < max_dd_pct:
                max_dd_pct = dd_pct

            # Check mission completion
            mission_completed = bool(state.get("mission_completed", False))
            last_level_thr = levels_cfg.last_level_threshold()
            if not mission_completed and eq >= last_level_thr and last_level_thr > 0:
                mission_completed = True
                tg.info(
                    "🎉 MISSION COMPLETE — equity reached target level.\n"
                    f"Equity: {eq} USDT (>= {last_level_thr})\n"
                    "Breaker should now block new trades for this subaccount."
                )

            # Goal logic
            session_goal = str(state.get("session_goal", "NEXT_TIER"))
            session_goal_hit = bool(state.get("session_goal_hit", False))

            # Level up: progress-only notification (does NOT end session)
            if lvl_idx > prev_lvl_idx:
                tg.info(
                    "📈 LEVEL UP — progress marker.\n"
                    f"From L{prev_lvl_idx + 1} to L{lvl_idx + 1} (>= {lvl_label} USDT).\n"
                    "Session continues until a new TIER is reached."
                )

            # Tier up: THIS ends the session (session_goal_hit)
            if (
                not session_goal_hit
                and session_goal.upper() == "NEXT_TIER"
                and tier_label != prev_tier_label
            ):
                session_goal_hit = True
                tg.info(
                    "🛑 SESSION GOAL HIT — TIER UP.\n"
                    f"From Tier {prev_tier_label} to Tier {tier_label} "
                    f"(max trades now {tier_max_trades}).\n"
                    "Trading session should stop; guard can now block new trades."
                )

            # Periodic log
            now = time.time()
            if now - last_log_ts > 60.0:
                last_log_ts = now
                log.info(
                    "[CarMission] equity=%s level=L%d(%s) tier=%s max_trades=%d "
                    "peak=%s dd=%.2f%% hours_left=%.1f completed=%s session_goal_hit=%s",
                    eq,
                    lvl_idx + 1,
                    lvl_label,
                    tier_label,
                    tier_max_trades,
                    peak_equity,
                    dd_pct,
                    hours_remaining,
                    mission_completed,
                    session_goal_hit,
                )

            # Persist state
            state.update(
                {
                    "equity_now": float(eq),
                    "level_index": lvl_idx,
                    "level_label": lvl_label,
                    "tier": tier_label,
                    "tier_max_trades": tier_max_trades,
                    "peak_equity": str(peak_equity),
                    "max_drawdown_pct": max_dd_pct,
                    "mission_start_ts": mission_start_ts,
                    "hours_elapsed": elapsed_hours,
                    "hours_remaining": hours_remaining,
                    "mission_completed": mission_completed,
                    "last_level_index": lvl_idx,
                    "last_tier": tier_label,
                    "session_goal": session_goal,
                    "session_goal_hit": session_goal_hit,
                    "last_day": state.get("last_day", today),
                    "sub_uid": CAR_MISSION_SUB_UID,
                    "label": CAR_MISSION_LABEL,
                }
            )
            save_state(state)

            time.sleep(POLL_SECONDS)

        except KeyboardInterrupt:
            log.info("[CarMission] KeyboardInterrupt, stopping.")
            break
        except Exception as e:
            log.exception("[CarMission] loop error: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    main_loop()
