#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Router v1.0 (DRY-RUN)

Role
----
Reads AI "actions" from the AI Action Bus (JSONL file) and routes them
for inspection / logging only.

NO REAL ORDERS ARE PLACED HERE.

For now, this is a DRY-RUN notification daemon:
  • Tails state/ai_actions.jsonl (or AI_ACTIONS_PATH)
  • Parses each new line as an "envelope" with:
        {
          "ts_ms": ...,
          "source": "ai_pilot",
          "label": "main",
          "dry_run": true,
          "action": { ... }
        }
  • Formats and (optionally) sends a Telegram summary

Later versions can:
  • Call an executor in DRY-RUN first
  • Apply guardrails, throttles, routing by label/type, etc.

Env
---
AI_ROUTER_ENABLED        (default: "true")
AI_ROUTER_POLL_SECONDS   (default: "2")
AI_ROUTER_SEND_TG        (default: "true")
ACCOUNT_LABEL            (default: "main")

Uses ACTION_LOG_PATH from app.core.ai_action_bus, so AI_ACTIONS_PATH controls
the file location.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import orjson

# Logging
try:
    from app.core.log import get_logger
except Exception:  # pragma: no cover
    import logging, sys

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_

logger = get_logger("ai_action_router")

# Core helpers
from app.core.flashback_common import (
    send_tg,
    record_heartbeat,
    alert_bot_error,
)

# Use the same path as ai_action_bus
from app.core.ai_action_bus import ACTION_LOG_PATH


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

AI_ROUTER_ENABLED: bool = _env_bool("AI_ROUTER_ENABLED", "true")
POLL_SECONDS: int = _env_int("AI_ROUTER_POLL_SECONDS", "2")
SEND_TG: bool = _env_bool("AI_ROUTER_SEND_TG", "true")


# ---------------------------------------------------------------------------
# File tailing helpers
# ---------------------------------------------------------------------------

def _iter_new_envelopes(
    path: Path,
    start_offset: int,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Read new JSONL rows from 'path' starting at byte offset 'start_offset'.

    Returns (new_offset, envelopes).
    """
    envelopes: List[Dict[str, Any]] = []

    if not path.exists():
        return start_offset, envelopes

    new_offset = start_offset
    try:
        with path.open("rb") as f:
            f.seek(start_offset)
            for line in f:
                new_offset += len(line)
                line = line.strip()
                if not line:
                    continue
                try:
                    env = orjson.loads(line)
                    if isinstance(env, dict):
                        envelopes.append(env)
                except Exception:
                    # Bad JSON line: skip
                    continue
    except Exception as e:
        alert_bot_error("ai_action_router", f"read error: {e}", "ERROR")
        return start_offset, []

    return new_offset, envelopes


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _fmt_env_to_text(env: Dict[str, Any]) -> str:
    """
    Turn an action envelope into a short human-readable summary.
    """
    ts_ms = env.get("ts_ms")
    src = env.get("source", "unknown")
    label = env.get("label", "unknown")
    dry = env.get("dry_run", True)
    act = env.get("action") or {}

    act_type = str(act.get("type", "advice_only"))
    symbol = act.get("symbol") or act.get("sym") or "N/A"
    side = act.get("side") or act.get("direction") or "N/A"
    size = act.get("size") or act.get("qty") or "N/A"
    reason = act.get("reason") or act.get("tag") or act.get("policy") or "n/a"
    extra = act.get("extra") or {}

    # Basic line
    lines: List[str] = []
    lines.append("🤖 AI Action Router")
    lines.append(f"  • label   : {label}")
    lines.append(f"  • source  : {src}")
    lines.append(f"  • dry_run : {dry}")
    lines.append(f"  • type    : {act_type}")
    lines.append(f"  • symbol  : {symbol}")
    lines.append(f"  • side    : {side}")
    lines.append(f"  • size    : {size}")
    lines.append(f"  • reason  : {reason}")

    # Some optional structured fields we might care about later
    for key in ("entry_price", "sl_price", "tp_price", "rr", "confidence"):
        if key in act:
            lines.append(f"  • {key:11s}: {act[key]}")

    # Short extra dump if present
    if isinstance(extra, dict) and extra:
        # Truncate keys a bit to avoid walls of text
        keys = list(extra.keys())[:5]
        lines.append("  • extra   : " + ", ".join(f"{k}={extra[k]}" for k in keys))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def loop() -> None:
    """
    Main router loop.

    Every POLL_SECONDS:
      - Tail ACTION_LOG_PATH for new envelopes
      - Render them
      - Optionally send to Telegram (DRY-RUN notification only)
    """
    if not AI_ROUTER_ENABLED:
        logger.warning("AI Action Router disabled via AI_ROUTER_ENABLED=false. Exiting.")
        return

    logger.info(
        "AI Action Router starting (label=%s, poll=%ss, send_tg=%s, path=%s)",
        ACCOUNT_LABEL,
        POLL_SECONDS,
        SEND_TG,
        ACTION_LOG_PATH,
    )

    # Start from EOF so we don't spam historical actions on first start.
    try:
        if ACTION_LOG_PATH.exists():
            offset = ACTION_LOG_PATH.stat().st_size
        else:
            offset = 0
    except Exception:
        offset = 0

    # Optional startup ping
    try:
        send_tg(
            f"📡 AI Action Router online for label={ACCOUNT_LABEL} "
            f"(poll={POLL_SECONDS}s, send_tg={SEND_TG})"
        )
    except Exception:
        pass

    while True:
        record_heartbeat("ai_action_router")

        try:
            offset, envelopes = _iter_new_envelopes(ACTION_LOG_PATH, offset)
            if envelopes:
                logger.info("AI Action Router read %d new actions", len(envelopes))

                if SEND_TG:
                    for env in envelopes:
                        try:
                            text = _fmt_env_to_text(env)
                            send_tg(text)
                        except Exception as e:
                            alert_bot_error("ai_action_router", f"tg send error: {e}", "WARN")

        except Exception as e:
            alert_bot_error("ai_action_router", f"loop error: {e}", "ERROR")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    loop()
