#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Config Validation

Run a series of checks at supervisor startup:
  - Required env vars
  - Existence + parse of strategies.yaml, bots.yaml, exit_profiles.yaml
"""

import os
from typing import List, Tuple

import yaml


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CFG_DIR = os.path.join(PROJECT_ROOT, "config")

REQUIRED_ENV_KEYS = [
    "BYBIT_MAIN_READ_KEY",
    "BYBIT_MAIN_READ_SECRET",
    "BYBIT_MAIN_TRADE_KEY",
    "BYBIT_MAIN_TRADE_SECRET",
    "TG_TOKEN_MAIN",
    "TG_CHAT_MAIN",
]


def _check_env() -> List[str]:
    missing = []
    for key in REQUIRED_ENV_KEYS:
        if not os.getenv(key):
            missing.append(f"Missing env: {key}")
    return missing


def _check_yaml(path: str, label: str) -> List[str]:
    if not os.path.exists(path):
        return [f"{label} not found at {path}"]
    try:
        with open(path, "r", encoding="utf-8") as f:
            yaml.safe_load(f)
    except Exception as e:
        return [f"{label} parse error: {e}"]
    return []


def validate_config() -> Tuple[bool, List[str]]:
    errors: List[str] = []

    errors.extend(_check_env())

    strategies_path = os.path.join(CFG_DIR, "strategies.yaml")
    bots_path = os.path.join(CFG_DIR, "bots.yaml")
    exit_profiles_path = os.path.join(CFG_DIR, "exit_profiles.yaml")

    errors.extend(_check_yaml(strategies_path, "strategies.yaml"))
    errors.extend(_check_yaml(bots_path, "bots.yaml"))
    errors.extend(_check_yaml(exit_profiles_path, "exit_profiles.yaml"))

    return (len(errors) == 0, errors)
