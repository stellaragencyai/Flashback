#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Sample AI Policy (toy)

This is a *toy* policy for ai_pilot:

- Input:  ai_state dict from ai_pilot._build_ai_state()
- Output: list of "actions" (pure metadata, no direct trading)

Schema for actions (for now, informal / advisory-only):
  {
    "type": "advice_only",
    "reason": "sample_policy",
    "label": "<ACCOUNT_LABEL>",
    "symbol": "BTCUSDT",
    "side": "Buy",
    "size": "0.005",
    "dry_run": true,
  }

ai_pilot currently:
  - Calls evaluate_state(ai_state) when AI_PILOT_SAMPLE_POLICY=true
  - Only logs the number of actions; no execution is wired yet.
"""

from __future__ import annotations

from typing import Any, Dict, List


def evaluate_state(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Minimal reference policy.

    Logic (deliberately simple):
      - For every open position in ai_state["positions"], emit an "advice_only"
        action summarizing the position.
      - No orders, no signals, no risk logic. This is just to exercise the
        plumbing and telemetry paths.

    Parameters
    ----------
    ai_state : dict
        State built by ai_pilot._build_ai_state().

    Returns
    -------
    List[Dict[str, Any]]
        A list of advisory "actions". Execution is the job of a future
        ai_action_router.
    """
    actions: List[Dict[str, Any]] = []

    positions = ai_state.get("positions") or []
    label = str(ai_state.get("label", "unknown"))
    dry_run = bool(ai_state.get("dry_run", True))

    if not isinstance(positions, list):
        return actions

    for p in positions:
        if not isinstance(p, dict):
            continue

        symbol = p.get("symbol")
        side = p.get("side")
        size = p.get("size")

        if not symbol or not side or size in (None, "", 0, "0"):
            continue

        actions.append(
            {
                "type": "advice_only",
                "reason": "sample_policy",
                "label": label,
                "symbol": str(symbol),
                "side": str(side),
                "size": str(size),
                "dry_run": dry_run,
            }
        )

    return actions
