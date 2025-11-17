#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Core Config

Provides a central `settings` object with important paths.
"""

from __future__ import annotations

from pathlib import Path
import os


class Settings:
    def __init__(self) -> None:
        # Project root: .../Flashback
        self.ROOT: Path = Path(__file__).resolve().parents[1]

        # State directory (for cursors, db, etc.)
        self.STATE_DIR: Path = self.ROOT / "state"
        self.STATE_DIR.mkdir(parents=True, exist_ok=True)

        # Database path (if/when needed by db layer)
        db_env = os.getenv("DB_PATH")
        if db_env:
            self.DB_PATH: Path = Path(db_env)
        else:
            self.DB_PATH: Path = self.STATE_DIR / "flashback.db"


settings = Settings()
