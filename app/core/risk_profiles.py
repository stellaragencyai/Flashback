#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Risk Profiles

Loads named risk templates from config/risk_profiles.yaml.

Each profile:
  {
    "risk_per_trade_pct": float,
    "max_concurrent_risk_pct": float
  }
"""

from pathlib import Path
from typing import Dict, Any

import yaml

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
RISK_CFG = ROOT / "config" / "risk_profiles.yaml"

_cache: Dict[str, Any] = {}
_loaded = False


class RiskProfileError(Exception):
    pass


def _load_all() -> None:
    global _cache, _loaded
    if _loaded:
        return
    if not RISK_CFG.exists():
        raise RiskProfileError(f"Risk profile config not found: {RISK_CFG}")
    data = yaml.safe_load(RISK_CFG.read_text(encoding="utf-8")) or {}
    profiles = data.get("profiles") or {}
    if not isinstance(profiles, dict):
        raise RiskProfileError("Invalid risk_profiles.yaml: 'profiles' must be a mapping.")
    _cache = profiles
    _loaded = True


def get_risk_profile(name: str) -> Dict[str, float]:
    _load_all()
    key = str(name).upper()
    if key not in _cache:
        raise RiskProfileError(f"Risk profile not found: {key}")
    prof = _cache[key]
    return {
        "risk_per_trade_pct": float(prof.get("risk_per_trade_pct", 0.0)),
        "max_concurrent_risk_pct": float(prof.get("max_concurrent_risk_pct", 0.0)),
    }
