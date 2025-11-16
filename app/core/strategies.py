#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Strategy Registry

Central access point for per-subaccount strategy config.

Reads:
    STRATEGY_CONFIG_PATH (from .env, defaults to config/strategies.yaml)
    SUB_LABELS          (from .env, e.g.
                         524630315:Sub1_Trend,524633243:Sub2_BO,...)

Exposes:
    all_sub_strategies()          -> List[dict]
    get_strategy_for_sub(sub_uid) -> Optional[dict]
    get_strategy_by_name(name)    -> Optional[dict]
    all_sub_uids()                -> List[str]
    get_sub_label(sub_uid)        -> str
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, List

import yaml

# ROOT = project root (.../Flashback)
ROOT = Path(__file__).resolve().parents[2]

_CONFIG_PATH = os.getenv("STRATEGY_CONFIG_PATH", "config/strategies.yaml")
CONFIG_PATH = (ROOT / _CONFIG_PATH).resolve()

# SUB_LABELS env example:
#   SUB_LABELS=524630315:Sub1_Trend,524633243:Sub2_BO,...
_SUB_LABELS_RAW = os.getenv("SUB_LABELS", "")

_cache: Optional[Dict[str, Any]] = None
_label_map: Optional[Dict[str, str]] = None


def _load() -> Dict[str, Any]:
    """
    Load the YAML strategy config once and cache it.
    """
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
    """
    Return the raw list of subaccount strategy dictionaries
    from strategies.yaml under key 'subaccounts'.
    """
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


def get_strategy_by_name(name: str) -> Optional[Dict[str, Any]]:
    """
    Lookup a strategy by its 'name' field (e.g., 'Sub1_Trend').
    Case-sensitive by default.
    """
    target = str(name)
    for strat in all_sub_strategies():
        if str(strat.get("name")) == target:
            return strat
    return None


def all_sub_uids() -> List[str]:
    """
    Convenience: list all sub_uids defined in strategies.yaml as strings.
    """
    uids: List[str] = []
    for strat in all_sub_strategies():
        uid = strat.get("sub_uid")
        if uid is None:
            continue
        uids.append(str(uid))
    return uids


def _parse_label_map() -> Dict[str, str]:
    """
    Parse SUB_LABELS from the environment into a mapping:
        { "524630315": "Sub1_Trend", ... }
    """
    global _label_map
    if _label_map is not None:
        return _label_map

    raw = _SUB_LABELS_RAW.strip()
    mapping: Dict[str, str] = {}
    if not raw:
        _label_map = mapping
        return mapping

    # Format: "524630315:Sub1_Trend,524633243:Sub2_BO,..."
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for item in parts:
        if ":" not in item:
            continue
        uid_str, label = item.split(":", 1)
        uid_str = uid_str.strip()
        label = label.strip()
        if not uid_str or not label:
            continue
        mapping[uid_str] = label

    _label_map = mapping
    return mapping


def get_sub_label(sub_uid: str) -> str:
    """
    Get a human-readable label for a sub_uid, using SUB_LABELS from .env
    if available, otherwise fall back to 'sub-<uid>'.
    """
    uid_str = str(sub_uid)
    mapping = _parse_label_map()
    label = mapping.get(uid_str)
    if label:
        return label
    return f"sub-{uid_str}"
