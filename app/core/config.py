#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Core config

Central "settings" object shared across bots.
"""

from __future__ import annotations

from pathlib import Path
import os


class Settings:
    def __init__(self) -> None:
        # Project root = parent of this "core" package
        # e.g. C:/Users/nolan/Desktop/Flashback
        self.ROOT: Path = Path(__file__).resolve().parents[1]

        # Optional explicit DB path; fallback: ROOT/state/flashback.db
        default_db = self.ROOT / "state" / "flashback.db"
        db_env = os.getenv("DB_PATH")
        self.DB_PATH: Path = Path(db_env) if db_env else default_db

        # You can add more global settings later (env name, log level, etc.)


settings = Settings()
