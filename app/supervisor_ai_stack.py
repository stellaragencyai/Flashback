#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Stack Supervisor (WS + TP/SL + AI Pilot + AI Router)

Purpose
-------
Single entrypoint to run the "AI stack" for ONE ACCOUNT_LABEL:

    • ws_switchboard       (WS → positions_bus / orderbook_bus / trades_bus)
    • tp_sl_manager        (position-bus aware TP/SL manager)
    • ai_pilot             (AI brain reading ai_state_bus)
    • ai_action_router     (routes AI decisions into real actions)

Design
------
- One OS process per worker using multiprocessing.Process.
- Windows-safe: targets are TOP-LEVEL functions (no lambdas / closures).
- Supervisor:
    • Reads AI_STACK_ENABLE_* env flags.
    • Starts enabled workers.
    • Restarts workers that die.
    • Writes heartbeat via flashback_common.record_heartbeat("supervisor_ai_stack").

Env flags
---------
ACCOUNT_LABEL                       (default: "main")
AI_STACK_ENABLE_WS_SWITCHBOARD      (default: "true")
AI_STACK_ENABLE_TP_SL_MANAGER       (default: "true")
AI_STACK_ENABLE_AI_PILOT            (default: "true")
AI_STACK_ENABLE_AI_ACTION_ROUTER    (default: "true")
"""

from __future__ import annotations

import importlib
import os
import time
import multiprocessing as mp
from typing import Dict, List, Optional

from app.core.flashback_common import record_heartbeat, alert_bot_error

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

try:
    from app.core.log import get_logger
except Exception:
    import logging

    def get_logger(name: str) -> logging.Logger:  # type: ignore
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


logger = get_logger("supervisor_ai_stack")


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: str = "true") -> bool:
    raw = os.getenv(name, default)
    if raw is None:
        raw = default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if not raw:
        return default
    return raw.strip()


ACCOUNT_LABEL: str = _env_str("ACCOUNT_LABEL", "main")


# ---------------------------------------------------------------------------
# Worker spec
# ---------------------------------------------------------------------------

class WorkerSpec:
    def __init__(
        self,
        name: str,
        env_flag: str,
        module_path: str,
        callable_name: str,
        default_enabled: bool = True,
        extra_env: Optional[Dict[str, str]] = None,
    ) -> None:
        self.name = name
        self.env_flag = env_flag
        self.module_path = module_path
        self.callable_name = callable_name
        self.default_enabled = default_enabled
        self.extra_env = extra_env or {}

    def is_enabled(self) -> bool:
        return _env_bool(self.env_flag, "true" if self.default_enabled else "false")


WORKERS: List[WorkerSpec] = [
    WorkerSpec(
        name="ws_switchboard",
        env_flag="AI_STACK_ENABLE_WS_SWITCHBOARD",
        module_path="app.ws.ws_switchboard",
        callable_name="main",
        default_enabled=True,
        extra_env={"ACCOUNT_LABEL": ACCOUNT_LABEL},
    ),
    WorkerSpec(
        name="tp_sl_manager",
        env_flag="AI_STACK_ENABLE_TP_SL_MANAGER",
        module_path="app.bots.tp_sl_manager",
        callable_name="loop",
        default_enabled=True,
        extra_env={"ACCOUNT_LABEL": ACCOUNT_LABEL},
    ),
    WorkerSpec(
        name="ai_pilot",
        env_flag="AI_STACK_ENABLE_AI_PILOT",
        module_path="app.bots.ai_pilot",
        callable_name="loop",
        default_enabled=True,
        extra_env={"ACCOUNT_LABEL": ACCOUNT_LABEL},
    ),
    WorkerSpec(
        name="ai_action_router",
        env_flag="AI_STACK_ENABLE_AI_ACTION_ROUTER",
        module_path="app.tools.ai_action_router",
        callable_name="main",
        default_enabled=True,
        extra_env={"ACCOUNT_LABEL": ACCOUNT_LABEL},
    ),
]


# ---------------------------------------------------------------------------
# Worker entrypoint (top-level, Windows-safe)
# ---------------------------------------------------------------------------

def _worker_main(
    name: str,
    module_path: str,
    callable_name: str,
    extra_env: Optional[Dict[str, str]] = None,
) -> None:
    """
    Run ONE worker inside its own process.

    This function MUST be top-level to be picklable on Windows.
    """
    try:
        if extra_env:
            for k, v in extra_env.items():
                if v is not None:
                    os.environ[k] = str(v)

        hb_name = f"supervisor_worker_{name}"
        logger.info(
            "Worker '%s' starting (module=%s, callable=%s, ACCOUNT_LABEL=%s)",
            name,
            module_path,
            callable_name,
            os.getenv("ACCOUNT_LABEL", "main"),
        )
        record_heartbeat(hb_name)

        mod = importlib.import_module(module_path)
        fn = getattr(mod, callable_name, None)
        if not callable(fn):
            msg = (
                f"Worker '{name}' could not find callable '{callable_name}' "
                f"in '{module_path}'"
            )
            logger.error(msg)
            alert_bot_error("supervisor_ai_stack", msg, "ERROR")
            return

        while True:
            record_heartbeat(hb_name)
            try:
                fn()
                logger.warning(
                    "Worker '%s' callable returned; sleeping before restart.",
                    name,
                )
                time.sleep(3)
            except KeyboardInterrupt:
                logger.info("Worker '%s' interrupted by user, exiting.", name)
                break
            except Exception as e:
                msg = f"Worker '{name}' crashed: {e}"
                logger.exception(msg)
                alert_bot_error("supervisor_ai_stack", msg, "ERROR")
                time.sleep(3)
    except KeyboardInterrupt:
        logger.info("Worker '%s' interrupted at top-level, exiting.", name)
    except Exception as e:
        msg = f"Worker '{name}' fatal error at top-level: {e}"
        logger.exception(msg)
        alert_bot_error("supervisor_ai_stack", msg, "ERROR")


# ---------------------------------------------------------------------------
# Supervisor main loop
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info(
        "Flashback AI Stack Supervisor starting for ACCOUNT_LABEL=%s",
        ACCOUNT_LABEL,
    )

    record_heartbeat("supervisor_ai_stack")

    try:
        if mp.get_start_method(allow_none=True) is None:
            mp.set_start_method("spawn", force=True)
    except RuntimeError:
        pass

    procs: Dict[str, mp.Process] = {}
    enabled_specs: List[WorkerSpec] = [w for w in WORKERS if w.is_enabled()]

    if not enabled_specs:
        logger.warning("No AI stack workers enabled via env flags; nothing to run.")
        return

    logger.info(
        "Enabled workers: %s",
        [w.name for w in enabled_specs],
    )

    try:
        while True:
            record_heartbeat("supervisor_ai_stack")

            for spec in enabled_specs:
                p = procs.get(spec.name)
                if p is None or not p.is_alive():
                    if p is not None and not p.is_alive():
                        logger.warning(
                            "Worker '%s' died (exitcode=%s). Restarting...",
                            spec.name,
                            p.exitcode,
                        )
                    logger.info(
                        "Starting worker process: %s (pid=?)",
                        spec.name,
                    )
                    p = mp.Process(
                        target=_worker_main,
                        args=(
                            spec.name,
                            spec.module_path,
                            spec.callable_name,
                            spec.extra_env,
                        ),
                        name=f"fb_ai_{spec.name}",
                        daemon=False,
                    )
                    p.start()
                    logger.info(
                        "Worker %s started with pid=%s",
                        spec.name,
                        p.pid,
                    )
                    procs[spec.name] = p

            time.sleep(5.0)

    except KeyboardInterrupt:
        logger.info(
            "Supervisor received KeyboardInterrupt. Terminating workers..."
        )
        for name, p in procs.items():
            try:
                logger.info(
                    "Terminating worker %s (pid=%s)...",
                    name,
                    p.pid,
                )
                p.terminate()
            except Exception:
                pass
        logger.info("Supervisor exiting.")


if __name__ == "__main__":
    main()
