#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from pathlib import Path
from typing import Dict

import orjson

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR = ROOT / "state"
HB_PATH = STATE_DIR / "heartbeats.json"


def _load() -> Dict[str, float]:
    if not HB_PATH.exists():
        return {}
    try:
        return orjson.loads(HB_PATH.read_bytes())
    except Exception:
        return {}


def _save(data: Dict[str, float]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    HB_PATH.write_bytes(orjson.dumps(data))


def touch(name: str) -> None:
    hb = _load()
    hb[name] = time.time()
    _save(hb)
