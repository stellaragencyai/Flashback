#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Profile config (per ACCOUNT_LABEL)

Purpose
-------
Provide a single place to resolve AI-related configuration, with support for:

  - Global defaults (AI_* env vars)
  - Per-account overrides via suffix: _<ACCOUNT_LABEL_UPPER>

Examples
--------
ACCOUNT_LABEL=main

Env resolution order for allowed symbols:
  1) AI_ALLOWED_SYMBOLS_MAIN
  2) AI_ALLOWED_SYMBOLS
  3) default ""

Same pattern for:
  - AI_REQUIRE_WHITELIST[_LABEL]
  - AI_MAX_NOTIONAL_PCT[_LABEL]
  - AI_PILOT_POLL_SECONDS[_LABEL]
  - AI_PILOT_SYMBOLS[_LABEL]
  - AI_PILOT_DRY_RUN[_LABEL]
  - AI_PILOT_SAMPLE_POLICY[_LABEL]
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any, Dict, List, Optional


def _account_label() -> str:
    return os.getenv("ACCOUNT_LABEL", "main").strip() or "main"


def _env_with_label(base: str, label: str) -> Optional[str]:
    """
    Resolve env with optional <BASE>_<LABEL> override.

    Example:
      _env_with_label("AI_ALLOWED_SYMBOLS", "main")
        -> AI_ALLOWED_SYMBOLS_MAIN, then AI_ALLOWED_SYMBOLS
    """
    label_up = label.upper()
    specific = f"{base}_{label_up}"
    if specific in os.environ:
        return os.environ[specific]
    if base in os.environ:
        return os.environ[base]
    return None


def _env_bool(base: str, label: str, default: bool) -> bool:
    raw = _env_with_label(base, label)
    if raw is None:
        return default
    raw = raw.strip().lower()
    return raw in ("1", "true", "yes", "y")


def _env_int(base: str, label: str, default: int) -> int:
    raw = _env_with_label(base, label)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


def _env_decimal(base: str, label: str, default: str) -> Decimal:
    raw = _env_with_label(base, label)
    if raw is None:
        raw = default
    try:
        from decimal import Decimal as _D
        return _D(str(raw))
    except Exception:
        from decimal import Decimal as _D
        return _D(default)


def _env_symbols_list(base: str, label: str) -> List[str]:
    raw = _env_with_label(base, label)
    if raw is None:
        return []
    parts = [s.strip().upper() for s in raw.split(",") if s.strip()]
    return parts


def get_ai_profile(account_label: Optional[str] = None) -> Dict[str, Any]:
    """
    Build an AI profile dict for a given account label.

    Keys:
      - account_label       : str
      - allowed_symbols     : List[str]
      - require_whitelist   : bool
      - max_notional_pct    : Decimal
      - pilot_poll_seconds  : int
      - pilot_symbols       : List[str]
      - pilot_dry_run       : bool
      - pilot_sample_policy : bool
    """
    label = account_label or _account_label()

    allowed_symbols = _env_symbols_list("AI_ALLOWED_SYMBOLS", label)
    require_whitelist = _env_bool("AI_REQUIRE_WHITELIST", label, default=False)
    max_notional_pct = _env_decimal("AI_MAX_NOTIONAL_PCT", label, default="40")

    pilot_poll_seconds = _env_int("AI_PILOT_POLL_SECONDS", label, default=2)
    pilot_symbols = _env_symbols_list("AI_PILOT_SYMBOLS", label)
    pilot_dry_run = _env_bool("AI_PILOT_DRY_RUN", label, default=True)
    pilot_sample_policy = _env_bool("AI_PILOT_SAMPLE_POLICY", label, default=False)

    return {
        "account_label": label,
        "allowed_symbols": allowed_symbols,
        "require_whitelist": require_whitelist,
        "max_notional_pct": max_notional_pct,
        "pilot_poll_seconds": pilot_poll_seconds,
        "pilot_symbols": pilot_symbols,
        "pilot_dry_run": pilot_dry_run,
        "pilot_sample_policy": pilot_sample_policy,
    }


def get_current_ai_profile() -> Dict[str, Any]:
    """
    Shortcut for the current ACCOUNT_LABEL.
    """
    return get_ai_profile(_account_label())
