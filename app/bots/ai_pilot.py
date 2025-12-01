#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Pilot v2.0

Role
----
Thin coordinator sitting on top of:
  • ai_state_bus.build_ai_snapshot()       (state aggregation)
  • optional AI policies (e.g. ai_policy_sample.evaluate_state)
  • ai_action_bus.append_actions()         (writes AI "decisions" to a bus)
  • flashback_common.record_heartbeat()    (liveness for supervisor/status)

Energy:
  • DRY-RUN ONLY by default.
  • No direct order placement. Execution is delegated to ai_action_router
    or other executors later.

Env
---
ACCOUNT_LABEL                 (default: "main")
AI_PILOT_ENABLED              (default: "true")
AI_PILOT_POLL_SECONDS         (default: "3")
AI_PILOT_DRY_RUN              (default: "true")

# Policy toggles
AI_PILOT_SAMPLE_POLICY        (default: "false")  # uses app.ai.ai_policy_sample

# Action bus
AI_PILOT_WRITE_ACTIONS        (default: "false")  # if true, writes to ai_action_bus

Usage
-----
Typically launched by supervisor_ai_stack:

    from app.bots.ai_pilot import loop as ai_pilot_loop

    ai_pilot_loop()
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

# Logging (robust)
try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

logger = get_logger("ai_pilot")

# Core helpers
from app.core.flashback_common import (
    send_tg,
    record_heartbeat,
    alert_bot_error,
)

from app.core.ai_state_bus import build_ai_snapshot
from app.core.ai_action_bus import append_actions

# Optional sample policy
try:
    from app.ai.ai_policy_sample import evaluate_state as sample_evaluate_state
except Exception:  # pragma: no cover
    sample_evaluate_state = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

AI_PILOT_ENABLED: bool = _env_bool("AI_PILOT_ENABLED", "true")
POLL_SECONDS: int = _env_int("AI_PILOT_POLL_SECONDS", "3")
DRY_RUN: bool = _env_bool("AI_PILOT_DRY_RUN", "true")

# Policy toggles
USE_SAMPLE_POLICY: bool = _env_bool("AI_PILOT_SAMPLE_POLICY", "false")

# Action bus toggle
WRITE_ACTIONS: bool = _env_bool("AI_PILOT_WRITE_ACTIONS", "false")


# ---------------------------------------------------------------------------
# State builders / helpers
# ---------------------------------------------------------------------------

def _build_ai_state() -> Dict[str, Any]:
    """
    Wrap ai_state_bus.build_ai_snapshot() into a policy-friendly dict.

    Policies do NOT need to know about internal bus layout; they get:
      - label, dry_run
      - account summary
      - flat positions list
      - light telemetry about buses (ages)
    """
    snap = build_ai_snapshot(
        focus_symbols=None,
        include_trades=False,
        trades_limit=0,
        include_orderbook=False,
    )

    account = snap.get("account") or {}
    pos_block = snap.get("positions") or {}
    positions_by_symbol = pos_block.get("by_symbol") or {}
    positions_list: List[Dict[str, Any]] = list(positions_by_symbol.values())

    buses = {
        "positions_bus_age_sec": snap.get("positions_bus_age_sec"),
        "orderbook_bus_age_sec": snap.get("orderbook_bus_age_sec"),
        "trades_bus_age_sec": snap.get("trades_bus_age_sec"),
    }

    ai_state: Dict[str, Any] = {
        "label": ACCOUNT_LABEL,
        "dry_run": DRY_RUN,
        "account": {
            "equity_usdt": account.get("equity_usdt"),
            "mmr_pct": account.get("mmr_pct"),
            "open_positions": len(positions_list),
        },
        "positions": positions_list,
        "buses": buses,
        "raw_snapshot": snap,  # keep for debugging / future richer policies
    }
    return ai_state


def _run_sample_policy(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Run the sample policy (if available) and return its actions list.
    """
    if not USE_SAMPLE_POLICY:
        return []
    if sample_evaluate_state is None:
        logger.warning("Sample policy is enabled but app.ai.ai_policy_sample is missing.")
        return []

    try:
        actions = sample_evaluate_state(ai_state)  # type: ignore[misc]
        if not isinstance(actions, list):
            return []
        return [a for a in actions if isinstance(a, dict)]
    except Exception as e:
        alert_bot_error("ai_pilot", f"sample_policy error: {e}", "ERROR")
        return []


def _dispatch_actions(actions: List[Dict[str, Any]], *, label: str) -> int:
    """
    Write actions to the ai_action_bus (if enabled).
    """
    if not actions:
        return 0
    if not WRITE_ACTIONS:
        return 0

    count = append_actions(
        actions,
        source="ai_pilot",
        label=label,
        dry_run=DRY_RUN,
    )
    return count


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def loop() -> None:
    """
    Main AI Pilot loop.

    Every POLL_SECONDS:
      - Record heartbeat
      - Build ai_state
      - Run enabled policies (sample for now)
      - Optionally write actions to ai_action_bus
    """
    if not AI_PILOT_ENABLED:
        logger.warning("AI Pilot is disabled via AI_PILOT_ENABLED=false. Exiting loop().")
        return

    mode_bits = []
    mode_bits.append("DRY-RUN" if DRY_RUN else "LIVE?")
    if USE_SAMPLE_POLICY:
        mode_bits.append("sample_policy")
    if WRITE_ACTIONS:
        mode_bits.append("write_actions")
    mode_str = ", ".join(mode_bits) if mode_bits else "idle"

    try:
        send_tg(
            f"🧠 AI Pilot started for label={ACCOUNT_LABEL} "
            f"({mode_str}, poll={POLL_SECONDS}s)"
        )
    except Exception:
        logger.info("AI Pilot started for label=%s (%s, poll=%ss)", ACCOUNT_LABEL, mode_str, POLL_SECONDS)

    logger.info(
        "AI Pilot loop starting (label=%s, poll=%ss, dry_run=%s, sample_policy=%s, write_actions=%s)",
        ACCOUNT_LABEL,
        POLL_SECONDS,
        DRY_RUN,
        USE_SAMPLE_POLICY,
        WRITE_ACTIONS,
    )

    while True:
        record_heartbeat("ai_pilot")
        t0 = time.time()

        try:
            ai_state = _build_ai_state()

            total_actions = 0

            # 1) Sample policy (advisory only for now)
            sample_actions = _run_sample_policy(ai_state)
            if sample_actions:
                written = _dispatch_actions(sample_actions, label=ACCOUNT_LABEL)
                total_actions += written

            # In future: chain additional policies here.

            if total_actions > 0:
                logger.info(
                    "AI Pilot emitted %d actions for label=%s (written=%d)",
                    len(sample_actions),
                    ACCOUNT_LABEL,
                    total_actions,
                )

        except Exception as e:
            alert_bot_error("ai_pilot", f"loop error: {e}", "ERROR")

        # Simple pacing
        elapsed = time.time() - t0
        sleep_sec = max(0.5, POLL_SECONDS - elapsed)
        time.sleep(sleep_sec)


if __name__ == "__main__":
    loop()
