#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Logging Core

Provides:
    - get_logger(name)  -> configured logger
    - bind_context(logger, **ctx) -> LoggerAdapter that injects context fields

All logs go to:
    - stdout
    - logs/flashback.log (rotated manually by you if needed)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict

# Try to get ROOT from config, otherwise infer from this file
try:
    from app.core.config import settings
except Exception:  # fallback for plain "core" imports
    try:
        from core.config import settings  # type: ignore
    except Exception:
        settings = None  # type: ignore


def _resolve_root() -> Path:
    if settings is not None and hasattr(settings, "ROOT"):
        try:
            root = Path(getattr(settings, "ROOT"))
            if root.exists():
                return root
        except Exception:
            pass
    # Fallback: project root is two levels up from this file: app/core/logger.py
    return Path(__file__).resolve().parents[2]


ROOT: Path = _resolve_root()
LOG_DIR: Path = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "flashback.log"


def _configure_root_logger() -> None:
    """
    Configure the root logger once.

    We only attach handlers a single time to avoid duplicate log lines
    whenever modules re-import this file.
    """
    root = logging.getLogger()
    # Use a custom flag to avoid double configuration
    if getattr(root, "_flashback_configured", False):  # type: ignore[attr-defined]
        return

    root.setLevel(logging.INFO)

    fmt = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    formatter = logging.Formatter(fmt)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    # File handler
    try:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        root.addHandler(fh)
    except Exception:
        # If file handler explodes, keep console logging only.
        pass

    # Mark as configured
    setattr(root, "_flashback_configured", True)  # type: ignore[attr-defined]


def get_logger(name: str) -> logging.Logger:
    """
    Return a module-level logger with Flashback formatting & handlers attached.
    """
    _configure_root_logger()
    return logging.getLogger(name)


class _ContextAdapter(logging.LoggerAdapter):
    """
    Simple adapter that injects a static context dict into `extra`.

    Usage:
        log = get_logger("executor")
        log = bind_context(log, bot="executor_v2", sub_uid="main")
        log.info("Placed order", extra={"symbol": "BTCUSDT"})
    """

    def __init__(self, logger: logging.Logger, context: Dict[str, Any]) -> None:
        super().__init__(logger, context)

    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {})
        # Merge base context into any user-provided extra
        merged = dict(self.extra)
        merged.update(extra)
        kwargs["extra"] = merged
        return msg, kwargs


def bind_context(logger: logging.Logger, **context: Any) -> logging.LoggerAdapter:
    """
    Wrap a logger with extra context fields.

    Example:
        log = get_logger("executor")
        log = bind_context(log, bot="executor_v2", sub_uid="main")
        log.info("Hello")  # logs with those context fields in `extra`
    """
    _configure_root_logger()
    return _ContextAdapter(logger, context)
