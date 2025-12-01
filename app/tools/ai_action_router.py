#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Router v0.1

Purpose
-------
Very first, safe version of the AI Action Router.

- Watches a JSONL actions file produced by ai_pilot for a single ACCOUNT_LABEL.
- Logs any new actions it sees.
- Does NOT place orders yet (no executor integration in this version).

Supervisor expects this module to exist as: app.tools.ai_action_router
and to expose a main() function and a normal __main__ entrypoint.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import orjson

# ---------------------------------------------------------------------------
# Logger (robust import)
# ---------------------------------------------------------------------------

try:
    import importlib

    _log_module = importlib.import_module("app.core.log")
    get_logger = getattr(_log_module, "get_logger")
except Exception:
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


logger = get_logger("ai_action_router")

# ---------------------------------------------------------------------------
# Env / paths
# ---------------------------------------------------------------------------

ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

# Where ai_pilot writes actions; directory only. File name is per-label.
# Example final path: state/ai_actions/actions_main.jsonl
ACTIONS_DIR = Path(os.getenv("AI_ACTIONS_DIR", "state/ai_actions"))
ACTIONS_DIR.mkdir(parents=True, exist_ok=True)

ACTIONS_FILE: Path = ACTIONS_DIR / f"actions_{ACCOUNT_LABEL}.jsonl"

POLL_SECONDS: int = int(os.getenv("AI_ACTION_ROUTER_POLL_SECONDS", "2"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_new_actions(last_size: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    Very simple "tail" reader on the JSONL actions file.

    - last_size is the previous file size in bytes.
    - Returns (new_actions, new_file_size).
    """
    if not ACTIONS_FILE.exists():
        return [], last_size

    raw = ACTIONS_FILE.read_bytes()
    new_size = len(raw)

    if new_size <= last_size:
        # No new data
        return [], new_size

    chunk = raw[last_size:]
    lines = chunk.splitlines()

    actions: List[Dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            obj = orjson.loads(line)
        except Exception:
            logger.debug("Skipping malformed action line: %r", line)
            continue
        if isinstance(obj, dict):
            actions.append(obj)

    return actions, new_size


# ---------------------------------------------------------------------------
# Core loop
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info(
        "AI Action Router starting for ACCOUNT_LABEL=%s, file=%s, poll=%ss",
        ACCOUNT_LABEL,
        ACTIONS_FILE,
        POLL_SECONDS,
    )

    last_size = 0

    while True:
        try:
            actions, last_size = _load_new_actions(last_size)
            if actions:
                logger.info(
                    "AI Action Router saw %d new actions for %s",
                    len(actions),
                    ACCOUNT_LABEL,
                )
                for a in actions:
                    logger.info("AI action: %s", a)
                # NOTE: This is where we will later:
                #   - translate AI actions -> executor signals
                #   - write to EXEC_SIGNALS_PATH for executor_v2
                # For now, we just log so the stack is safe & observable.

            time.sleep(POLL_SECONDS)

        except KeyboardInterrupt:
            logger.info("AI Action Router interrupted by user, exiting.")
            break
        except Exception as e:
            logger.exception("AI Action Router error: %s", e)
            time.sleep(3)


if __name__ == "__main__":
    main()
