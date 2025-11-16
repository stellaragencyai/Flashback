#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Strategy Registry

Central access point for per-subaccount strategy config.

Reads:
    STRATEGY_CONFIG_PATH (from .env, defaults to config/strategies.yaml)

Exposes:
    all_sub_strategies() -> List[dict]
    get_strategy_for_sub(sub_uid) -> Optional[dict]
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, List

import yaml

# ROOT = project root (.../Flashback)
ROOT = Path(__file__).resolve().parents[2]

_CONFIG_PATH = os.getenv("STRATEGY_CONFIG_PATH", "config/strategies.yaml")
CONFIG_PATH = (ROOT / _CONFIG_PATH).resolve()

_cache: Optional[Dict[str, Any]] = None


def _load() -> Dict[str, Any]:
    global _cache
    if _cache is not None:
        return _cache

    if not CONFIG_PATH.exists():
        # Fail loudly; this is a core part of the organism
        raise FileNotFoundError(f"Strategy config not found at {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Strategy config at {CONFIG_PATH} must be a mapping at top level.")

    _cache = data
    return _cache


def all_sub_strategies() -> List[Dict[str, Any]]:
    cfg = _load()
    subs = cfg.get("subaccounts", []) or []
    if not isinstance(subs, list):
        raise ValueError("`subaccounts` in strategy config must be a list.")
    return subs


def get_strategy_for_sub(sub_uid: str) -> Optional[Dict[str, Any]]:
    """
    Return the strategy dict for a given sub_uid (string or int),
    or None if not found.
    """
    sub_uid_str = str(sub_uid)
    for strat in all_sub_strategies():
        if str(strat.get("sub_uid")) == sub_uid_str:
            return strat
    return None
